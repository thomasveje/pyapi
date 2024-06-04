import asyncio, websockets, random, logging, threading, time, functools, traceback, grpc, json, datetime 
from urllib.parse import urlparse
from proto import base_pb2, queues_pb2, workitems_pb2, querys_pb2, watch_pb2
from proto.base_pb2_grpc import FlowServiceStub
class protowrap():
    pending:dict = {}
    streams:dict = {}
    @staticmethod
    def timestamp():
            now = datetime.datetime.now()
            return  now.strftime("[%H:%M:%S] ")
    @staticmethod
    def Connect(client):
        uri = urlparse(client.url)
        client.scheme = uri.scheme
        if(uri.scheme == "grpc"):
            threading.Thread(target=protowrap.__grpc_listen_for_messages, args=(client, ), daemon=True).start()
        if(uri.scheme == "ws" or uri.scheme == "wss"):
            thread = threading.Thread(target=protowrap.runws, args=(client, ), daemon=True)
            thread.start()
            # threading.Thread(target=protowrap.__ws_connect_and_listen, args=(client, ), daemon=True).start()
            # listen_task = asyncio.create_task(protowrap.__ws_connect_and_listen(client))
            # asyncio.run(protowrap.__ws_connect_and_listen(client))
    @staticmethod
    def runws(client):
        while True:
            try:
                # asyncio.run_coroutine_threadsafe(protowrap.__ws_connect_and_listen(client), asyncio.get_event_loop())
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(protowrap.__ws_connect_and_listen(client))
                finally:
                    loop.close()
            except websockets.exceptions.ConnectionClosedOK as e:
                # print(repr(e))
                # traceback.print_tb(e.__traceback__)
                pass
            except Exception as e:
                if(client.connected == True):
                    client.connected = False
                    print(repr(e))
                    traceback.print_tb(e.__traceback__)
                pass
            logging.debug(f"Close channels")
            client.messagequeues = {}
            client.watches = {}
            err = ValueError("Channel closed")
            for id in protowrap.pending:
                try:
                    client.loop.call_soon_threadsafe(protowrap.pending[id].set_exception, err)
                except Exception as e:
                    pass
            for id in protowrap.pending:
                try:
                    client.loop.call_soon_threadsafe(protowrap.pending[id].set_exception, err)
                except Exception as e:
                    pass
            sleep = random.randint(1, 5)
            logging.debug(f"Sleeping for {sleep} seconds")
            time.sleep(sleep)
            # if(client.chan != None): client.chan.close()
    @staticmethod
    async def __ws_connect_and_listen(client):
        uri = urlparse(client.url)
        logging.info(f"Connecting to {uri.hostname}:{uri.port}")
        websocket: websockets.WebSocketClientProtocol = None
        websocket = await websockets.connect(client.url, max_size=65536*65536)
        client.seq = 0
        # async with websockets.connect(client.url) as websocket:
        print(f"Connected to {uri.hostname}")
        # sender_task = asyncio.create_task(protowrap.__ws_response_iterator(client, websocket))
        threading.Thread(target=protowrap.__ws_response_iterator, args=(client, websocket ), daemon=True).start()
        asyncio.run_coroutine_threadsafe(client.onconnected(client), client.loop)
        while websocket.open:
            try:
                # msg = await asyncio.shield(websocket.recv())
                msg = await asyncio.wait_for(websocket.recv(), timeout=None)
            except asyncio.TimeoutError: # ConnectionClosedOK
                print("Timed out waiting for message from server")
                continue
            except websockets.exceptions.ConnectionClosedOK as e:
                continue
            except Exception as e:
                print(repr(e))
                traceback.print_tb(e.__traceback__)
                continue
            size = int.from_bytes(msg, 'little', signed=False)
            # print(f"{protowrap.timestamp()} Received {len(msg)} bytes ( expect a package of {size} bytes)")
            msg = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            # print(f"{protowrap.timestamp()} Received {len(msg)} bytes")
            while(len(msg) < size):
                _tmp = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                # print(f"{protowrap.timestamp()} Received {len(_tmp)} bytes")
                msg += _tmp

            # print(f"{protowrap.timestamp()} Recevied msg {size} bytes")

            message = base_pb2.Envelope()
            message.ParseFromString(msg);

            # print(f"{protowrap.timestamp()} Process msg {message.command}")

            protowrap.parse_message(client, message)

        # sender_task.cancel() # cancel the sender task when we're done
        print("Connection to server closed")
    def __ws_response_iterator(client, websocket: websockets.WebSocketClientProtocol):
        while True:
            try:
                if(websocket.open == False):
                    break
                msg = client.grpcqueue.get()
                msg.seq = client.seq
                bytes = msg.SerializeToString()
                # print(f"{protowrap.timestamp()} Sending {msg.seq}:{msg.command} of {len(bytes)} bytes")
                if(websocket.open == False): # wtf ? how can it be closed here?
                    print(f"Not opened, resubmitting message {msg.seq}:{msg.command} of {len(bytes)} bytes")
                    client.grpcqueue.put(msg)
                    break
                asyncio.run(websocket.send(len(bytes).to_bytes(4, 'little', signed=False)))
                asyncio.run(websocket.send(bytes))
                client.seq += 1
            except Exception as e:
                break
    def __grpc_connect_and_listen(client, itr):
        try:
            uri = urlparse(client.url)
            grpc_max_receive_message_length = client.grpc_max_receive_message_length
            if(uri.port == 443 or uri.port == "443"):
                logging.info(f"Connecting to {uri.hostname}:{uri.port} using ssl credentials")
                credentials = grpc.ssl_channel_credentials()
                client.chan = grpc.secure_channel(f"{uri.hostname}:{uri.port}", credentials,
                                                  options=(
                                                    ('grpc.ssl_target_name_override', uri.hostname),
                                                    ('grpc.max_receive_message_length', grpc_max_receive_message_length),))
            else:
                logging.info(f"Connecting to {uri.hostname}:{uri.port}")
                client.chan = grpc.insecure_channel(f"{uri.hostname}:{uri.port}", options=(('grpc.max_receive_message_length', grpc_max_receive_message_length),))
            fut = grpc.channel_ready_future(client.chan)
            while not fut.done():
                logging.debug("channel is not ready")
                time.sleep(1)
            client.connected = True
            logging.info(f"Connected to {uri.hostname}:{uri.port}")
            asyncio.run_coroutine_threadsafe(client.onconnected(client), client.loop)
            logging.debug(f"Create stub and connect streams")
            stub = FlowServiceStub(client.chan)
            for message in stub.SetupStream(itr):
                logging.debug(f"RCV[{message.id}][{message.rid}][{message.command}]")
                protowrap.parse_message(client, message)
        except Exception as e:
            if(client.connected == True):
                client.connected = False
                print(repr(e))
                traceback.print_tb(e.__traceback__)
            pass
        logging.debug(f"Close channels")
        client.messagequeues = {}
        client.watches = {}
        for id in protowrap.pending:
            err = ValueError("Channel closed")
            client.loop.call_soon_threadsafe(protowrap.pending[id].set_exception, err)
        for id in protowrap.pending:
            err = ValueError("Channel closed")
            client.loop.call_soon_threadsafe(protowrap.pending[id].set_exception, err)
        client.chan.close()
    def __grpc_request_iterator(client, connectonid:str):
        try:
            logging.debug(f"Waiting for message for connecton id {connectonid}")
            message = client.grpcqueue.get()
            if(connectonid != protowrap.connectonid):
                client.grpcqueue.put(message)
                return None
            logging.debug(f"Process sending message for connecton id {connectonid}")
            if(message.id == None or message.id == ""): message.id = str(next(protowrap.uniqueid()))
            logging.debug(f"SND[{message.id}][{message.rid}][{message.command}]")
            return(message)
        except Exception as e:
            print(repr(e))
            traceback.print_tb(e.__traceback__)
            client.chan.close()
            pass        
    @staticmethod
    def __grpc_listen_for_messages(client):
        while True:
            protowrap.connectonid = str(next(protowrap.uniqueid()))
            count = 0
            logging.debug(f"Estabilish connecton id {protowrap.connectonid}")
            protowrap.__grpc_connect_and_listen(client,
                iter(functools.partial(protowrap.__grpc_request_iterator, client, protowrap.connectonid), None)
            )
            protowrap.connected = False
            count += 1
            sleep = random.randint(1, 5)
            logging.debug(f"Sleeping for {sleep} seconds before reconnecting ({count} attempts)")
            time.sleep(sleep)
    @staticmethod
    def RPC(client, request:base_pb2.Envelope, id:str = ""):
        if(id == ""):
            id = str(next(protowrap.uniqueid()))
        request.id = id
        future = asyncio.Future()
        protowrap.pending[id] = future
        protowrap.sendMesssag(client, request, id)
        return future
    @staticmethod
    def sendMesssag(client, request:base_pb2.Envelope, id:str):
        if(request.id == None or request.id == ""):
            id = str(next(protowrap.uniqueid()))
            request.id = id
        if(client.scheme == "grpc"):
            client.grpcqueue.put(request)
        elif(client.scheme == "ws" or client.scheme == "wss"):
            client.grpcqueue.put(request)
        else:
            raise ValueError(f"Unknown scheme {client.scheme}")
    @staticmethod
    def SetStream(rid:str):
        protowrap.streams[rid] = bytearray(0)
    @staticmethod
    def uniqueid():
        protowrap.seed = random.getrandbits(32)
        while True:
            yield protowrap.seed
            protowrap.seed += 1
    @staticmethod
    async def DownloadFile(client, Id:str=None, Filename:str=None, OutPath: str=None):
        request = base_pb2.Envelope(command="download")
        request.data.Pack(base_pb2.DownloadRequest(filename=Filename,id=Id))
        rid = str(next(protowrap.uniqueid()))
        request.id = rid
        protowrap.SetStream(rid)
        result:base_pb2.DownloadResponse = await protowrap.RPC(client, request, rid)
        if(result.filename != None and result.filename != ""):
            if OutPath == None:
                Path = result.filename
            else:
                Path = OutPath + "/" + result.filename
            with open(Path, "wb") as out_file:
                out_file.write(protowrap.streams[rid])
        protowrap.streams.pop(rid, None)
        return result
    async def __handle_onmessage_callback(client, message, msg):
        reply = await client.onmessage(client, message.command, message.id, msg)
        if(reply != None and message.rid == ""):
            if(reply.command != "noop"):
                protowrap.sendMesssag(client, reply, reply.id)

    async def __handle_watchevent_callback(client, msg):
        doc = json.loads(msg.document)
        await client.watches[msg.id](client, msg.operation, doc)
    async def __handle_queueevent_callback(client, msg):
        payload = json.loads(msg.data)
        if("payload" in payload):
            payload = payload["payload"]
        payload = await client.messagequeues[msg.queuename](client, msg, payload)
        if(payload != None  and msg.replyto != None and msg.replyto != ""):
            reply = base_pb2.Envelope(command="queuemessage")
            res = json.dumps(payload)
            reply.data.Pack(queues_pb2.QueueMessageRequest(queuename=msg.replyto, data=res, striptoken=True, correlationId=msg.correlationId))
            protowrap.sendMesssag(client, reply, reply.id)
    def parse_message(client, message:base_pb2.Envelope):
        return protowrap.__parse_message(client, message=message)
    def __parse_message(client, message:base_pb2.Envelope):
        msg = protowrap.__Unpack(message)
        if(message.command == "refreshtoken"):
            client.jwt = msg.jwt
            client.user = msg.user
        elif(message.command == "queueevent" and msg.correlationId in protowrap.pending and msg.replyto == ""):
            client.loop.call_soon_threadsafe(protowrap.pending[msg.correlationId].set_result, msg)
            protowrap.pending.pop(msg.correlationId, None)
        elif(message.rid in protowrap.streams and message.command in ["beginstream", "stream", "endstream"]):
            # if(message.command == "beginstream"):
            # elif(message.command == "endstream"):
            if(message.command == "stream"):
                # protowrap[message.rid] += msg.data
                protowrap.streams[message.rid].extend(msg.data)
        elif(message.rid in protowrap.pending):
            if(message.command == "error"):
                #raise ValueError(msg.message)
                client.loop.call_soon_threadsafe(protowrap.pending[message.rid].set_exception, ValueError(f"SERVER ERROR {msg.message}\n{msg.stack}" ))
            else:
                client.loop.call_soon_threadsafe(protowrap.pending[message.rid].set_result, msg)
            protowrap.pending.pop(message.rid, None)
        else:
            if(message.command == "watchevent" and msg.id in client.watches):
                asyncio.run_coroutine_threadsafe(protowrap.__handle_watchevent_callback(client, msg), client.loop)
            elif(message.command == "queueevent" and msg.queuename in client.messagequeues):
                asyncio.run_coroutine_threadsafe(protowrap.__handle_queueevent_callback(client, msg), client.loop)
            elif(message.command == "ping" or message.command == "pong" or message.command == "queuemessagereply"):
                pass
                #time.sleep(1)
            else:
                asyncio.run_coroutine_threadsafe(protowrap.__handle_onmessage_callback(client, message, msg), client.loop)
    def Unpack(self, message:base_pb2.Envelope):
        return protowrap.__Unpack(message)
    def __Unpack(message:base_pb2.Envelope):
        if(message.command == "getelement"):
            msg = base_pb2.GetElementResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "signinreply"):
            msg = base_pb2.SigninResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "refreshtoken"):
            msg = base_pb2.RefreshToken()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "registerqueuereply"):
            msg = queues_pb2.RegisterQueueResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "queuemessagereply"):
            msg = queues_pb2.QueueMessageResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "queueevent"):
            msg = queues_pb2.QueueEvent()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "pushworkitemreply"):
            msg = workitems_pb2.PushWorkitemResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "popworkitemreply"):
            msg = workitems_pb2.PopWorkitemResponse()
            if(message.data.value != None and message.data.value != "" and message.data.value != b""):
                msg.ParseFromString(message.data.value);
                return msg
            else:
                return None
        elif(message.command == "updateworkitemreply"):
            msg = workitems_pb2.UpdateWorkitemResponse()
            if(message.data.value != None and message.data.value != "" and message.data.value != b""):
                msg.ParseFromString(message.data.value);
                return msg
            else:
                return None
        elif(message.command == "pong"):
            msg = base_pb2.PingResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "ping"):
            msg = base_pb2.PingRequest()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "error"):
            msg = base_pb2.ErrorResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "download"):
            msg = base_pb2.DownloadRequest()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "downloadreply"):
            msg = base_pb2.DownloadResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "stream"):
            msg = base_pb2.Stream()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "beginstream"):
            msg = base_pb2.BeginStream()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "endstream"):
            msg = base_pb2.EndStream()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "queryreply"):
            msg = querys_pb2.QueryResponse()
            msg.ParseFromString(message.data.value);
            return msg 
        elif(message.command == "listcollectionsreply"):
            msg = querys_pb2.ListCollectionsResponse()
            msg.ParseFromString(message.data.value);
            return msg 
        elif(message.command == "dropcollectionreply"):
            msg = querys_pb2.DropCollectionResponse()
            msg.ParseFromString(message.data.value);
            return msg 
        elif(message.command == "getdocumentversionreply"):
            msg = querys_pb2.GetDocumentVersionResponse()
            msg.ParseFromString(message.data.value);
            return msg 
        elif(message.command == "countreply"):
            msg = querys_pb2.CountResponse()
            msg.ParseFromString(message.data.value);
            return msg 
        elif(message.command == "aggregatereply"):
            msg = querys_pb2.AggregateResponse()
            msg.ParseFromString(message.data.value);
            return msg 
        elif(message.command == "insertonereply"):
            msg = querys_pb2.InsertOneResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "insertmanyreply"):
            msg = querys_pb2.InsertManyResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "updateonereply"):
            msg = querys_pb2.UpdateOneResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "updatedocumentreply"):
            msg = querys_pb2.UpdateDocumentResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "insertorupdateonereply"):
            msg = querys_pb2.InsertOrUpdateOneResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "insertorupdatemanyreply"):
            msg = querys_pb2.InsertOrUpdateManyResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "deleteonereply"):
            msg = querys_pb2.DeleteOneResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "deletemanyreply"):
            msg = querys_pb2.DeleteManyResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "watchreply"):
            msg = watch_pb2.WatchResponse()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "watchevent"):
            msg = watch_pb2.WatchEvent()
            msg.ParseFromString(message.data.value);
            return msg
        elif(message.command == "unwatchreply"):
            msg = watch_pb2.UnWatchResponse()
            msg.ParseFromString(message.data.value);
            return msg
        else:
            logging.error(f"Got unknown {message.command} message")
            return None