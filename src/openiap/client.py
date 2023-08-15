from urllib.parse import urlparse
from queue import Queue
import gzip
import os
import json
import sys
from datetime import datetime
import asyncio
import time
import logging
import threading
import signal
from .protowrap import protowrap

from proto import base_pb2, queues_pb2, workitems_pb2, querys_pb2, watch_pb2


class Client():
    async def ainput(self, string: str) -> str:
        await asyncio.get_event_loop().run_in_executor(
                None, lambda s=string: sys.stdout.write(s+' '))
        return await asyncio.get_event_loop().run_in_executor(
                None, sys.stdin.readline)
    def __init__(self, url:str = "", grpc_max_receive_message_length:int = 4194304):
        self.url = url
        self.grpc_max_receive_message_length = grpc_max_receive_message_length
        self.scheme = ""
        self.replyqueue = ""
        self.jwt = ""
        if(self.url == None or self.url == ""): 
            self.url = os.environ.get("apiurl", "")
            if(self.url == ""): self.url = os.environ.get("grpcapiurl", "")
            self.jwt = os.environ.get("jwt", "")
            if(self.jwt == ""):
                uri = urlparse(self.url)
                if(uri.username == None or uri.username == "" or uri.password == None or uri.password == ""):
                    raise ValueError("No jwt environment variable and no credentials in url")
        if(self.url == None or self.url == ""): self.url = "grpc://grpc.app.openiap.io:443"
        self.connected = False
        self.loop = asyncio.get_event_loop()
        self.messagequeues = {}
        self.watches = {}
        self.grpcqueue = Queue()
        self.seq = 0
        protowrap.Connect(self)
        # threading.Thread(target=self.__listen_for_messages, daemon=True).start()
        threading.Thread(target=self.__server_pinger, daemon=True).start()
    def Close(self):
        if(self.connected == True):
            self.connected = False
            self.chan.close()
    async def onmessage(self, client, command, rid, message):
        logging.info(f"Got {command} message event")
    async def onconnected(self, client):
        logging.info(f"Connected")
        # uri = urlparse(self.url)
        # if(uri.username != None and uri.username != "" and uri.password != None and uri.password != ""):
        #     await self.Signin(uri.username, uri.password)
    def __server_pinger(self):
        count = 0
        while True:
            time.sleep(5)
            if(self.connected):
                message = base_pb2.Envelope(command="ping")
                self.grpcqueue.put(message)
            time.sleep(25)
            count += 1
    async def Signin(self, username:str=None, password:str=None, ping:bool=True, validateonly:bool=False, longtoken:bool=False):
        request = base_pb2.Envelope(command="signin")
        if(username == None and password==None):
            jwt = os.environ.get("jwt", "")
            if jwt == "": jwt = self.jwt
            if jwt != "" and jwt != None:
                username=jwt
            else:
                uri = urlparse(self.url)
                if(uri.username != None and uri.username != "" and uri.password != None and uri.password != ""):
                    username=uri.username
                    password=uri.password
        if(password== None or password == ""):
            request.data.Pack(base_pb2.SigninRequest(jwt=username, ping=ping, agent="python", version="0.0.33", validateonly=validateonly, longtoken=longtoken))
        else:
            request.data.Pack(base_pb2.SigninRequest(username=username, password=password, ping=ping, agent="python", version="0.0.33", validateonly=validateonly, longtoken=longtoken))
        result:base_pb2.SigninResponse = await protowrap.RPC(self, request)
        self.jwt = result.jwt
        self.user = result.user
        return result.user
    async def DownloadFile(self, Id:str=None, Filename:str=None, OutPath: str=None):
        result = await protowrap.DownloadFile(self, Id, Filename, OutPath)
        return result
    async def GetElement(self, xpath:str):
        request = base_pb2.Envelope(command="getelement")
        request.data.Pack(base_pb2.GetElementRequest(xpath=xpath))
        result:base_pb2.GetElementResponse = await protowrap.RPC(self, request)
        return result.xpath
    async def RegisterQueue(self, queuename:str, callback):
        request = base_pb2.Envelope(command="registerqueue")
        request.data.Pack(queues_pb2.RegisterQueueRequest(queuename=queuename))
        result:queues_pb2.RegisterQueueResponse = await protowrap.RPC(self, request)
        self.messagequeues[result.queuename] = callback
        if(self.replyqueue == ""):
            self.replyqueue = result.queuename
        return result.queuename
    async def QueueMessage(self, queuename:str, payload, correlationId=None, striptoken=True, rpc=False):
        future = None
        replyto = ""
        if(rpc==True and self.replyqueue != ""):
            correlationId = str(next(protowrap.uniqueid()))
            future = asyncio.Future()
            protowrap.pending[correlationId] = future
            replyto = self.replyqueue

        reply = base_pb2.Envelope(command="queuemessage")
        res = json.dumps(payload)
        reply.data.Pack(queues_pb2.QueueMessageRequest(queuename=queuename, data=res, striptoken=striptoken, correlationId=correlationId, replyto=replyto))
        self.grpcqueue.put(reply)
        if(future!=None):
            msg:queues_pb2.QueueMessageResponse = await future
            payload = json.loads(msg.data)
            if("payload" in payload):
                payload = payload["payload"]
            return payload
    async def PushWorkitem(self, wiq:str, name:str, payload:dict, files: any = None, wiqid:str = None, nextrun: datetime = None, priority: int = 2, compressed: bool = False):
        request = base_pb2.Envelope(command="pushworkitem")
        _files = []
        if(files != None):
            for filepath in files:
                filename = os.path.basename(filepath)
                if compressed == True:
                    with open(filepath, mode="rb") as content:
                        _files.append({"filename":filename, "compressed": compressed, "file": gzip.compress(content.read())})
                else:
                    with open(filepath, mode="rb") as content:
                        _files.append({"filename":filename, "compressed": compressed, "file": content.read()})
        q = workitems_pb2.PushWorkitemRequest(wiq=wiq,name=name, files=_files, wiqid=wiqid, nextrun=nextrun, priority=priority )
        q.payload = json.dumps(payload)
        request.data.Pack(q)
        result:workitems_pb2.PushWorkitemResponse = await protowrap.RPC(self, request)
        return result.workitem;
    async def PopWorkitem(self, wiq:str,includefiles:bool=False,compressed:bool=False):
        request = base_pb2.Envelope(command="popworkitem")
        request.data.Pack(workitems_pb2.PopWorkitemRequest(wiq=wiq,includefiles=includefiles,compressed=compressed))
        result:workitems_pb2.PopWorkitemResponse = await protowrap.RPC(self, request)
        if(result == None): return None
        return result.workitem;
    async def UpdateWorkitem(self, workitem, files: any = None, compressed:bool=False):
        request = base_pb2.Envelope(command="updateworkitem")
        _files = []
        for f in workitem.files:
            workitem.files.remove(f)
        if(files != None):
            for filepath in files:
                filename = os.path.basename(filepath)
                if compressed == True:
                    with open(filepath, mode="rb") as content:
                        _files.append({"filename":filename, "compressed": compressed, "file": gzip.compress(content.read())})
                else:
                    with open(filepath, mode="rb") as content:
                        _files.append({"filename":filename, "compressed": compressed, "file": content.read()})
        uwi = workitems_pb2.UpdateWorkitemRequest(workitem = workitem, files = _files);
        request.data.Pack(uwi)
        result:workitems_pb2.UpdateWorkitemResponse = await protowrap.RPC(self, request)
        if(result == None): return None
        return result.workitem;
    async def Query(self, collectionname:str="entities", query:dict={}, projection:dict={},top:int=100,skip:int=0, orderby=None, queryas:str=None):
        o = orderby
        if(not isinstance(o, str)): o = json.dumps(o)
        request = base_pb2.Envelope(command="query")
        request.data.Pack(querys_pb2.QueryRequest(
            collectionname=collectionname, query=json.dumps(query), projection=json.dumps(projection), top=top,skip=skip, orderby=o, queryas=queryas))
        result:querys_pb2.QueryResponse = await protowrap.RPC(self, request)
        if(result == None or result.results == None): return None
        return json.loads(result.results)
    async def ListCollections(self, includehist:bool=False):
        request = base_pb2.Envelope(command="listcollections")
        request.data.Pack(querys_pb2.ListCollectionsRequest(includehist=includehist))
        result:querys_pb2.ListCollectionsResponse = await protowrap.RPC(self, request)
        if(result == None or result.results == None): return None
        return json.loads(result.results)
    async def DropCollection(self, collectionname:str):
        request = base_pb2.Envelope(command="dropcollection")
        request.data.Pack(querys_pb2.DropCollectionRequest(collectionname=collectionname))
        await protowrap.RPC(self, request)
    async def GetDocumentVersion(self, id:str, collectionname:str, version:int = 0, decrypt:bool=True):
        request = base_pb2.Envelope(command="getdocumentversion")
        request.data.Pack(querys_pb2.GetDocumentVersionRequest(collectionname=collectionname, id=id, version=version, decrypt=decrypt))
        result:querys_pb2.GetDocumentVersionResponse = await protowrap.RPC(self, request)
        if(result == None or result.result == None or result.result == ""): return None
        return json.loads(result.result)
    async def Count(self, query:dict={}, collectionname:str="entities", queryas:str=None):
        request = base_pb2.Envelope(command="count")
        request.data.Pack(querys_pb2.CountRequest(
            collectionname=collectionname, query=json.dumps(query), queryas=queryas))
        result:querys_pb2.CountResponse = await protowrap.RPC(self, request)
        if(result == None): return 0
        return result.result
    async def Aggregate(self, aggregates:dict={}, collectionname:str="entities", queryas:str=None):
        request = base_pb2.Envelope(command="aggregate")
        request.data.Pack(querys_pb2.AggregateRequest(
            collectionname=collectionname, aggregates=json.dumps(aggregates), queryas=queryas))
        result:querys_pb2.AggregateResponse = await protowrap.RPC(self, request)
        if(result == None or result.results == None): return None
        return json.loads(result.results)
    async def InsertOne(self, item:dict, collectionname:str="entities"):
        request = base_pb2.Envelope(command="insertone")
        request.data.Pack(querys_pb2.InsertOneRequest(
            collectionname=collectionname, item=json.dumps(item)))
        result:querys_pb2.InsertOneResponse = await protowrap.RPC(self, request)
        if(result == None or result.result == None): return None
        return json.loads(result.result)
    async def InsertMany(self, items:list(), collectionname:str="entities", skipresults:bool=False):
        request = base_pb2.Envelope(command="insertmany")
        request.data.Pack(querys_pb2.InsertManyRequest(
            collectionname=collectionname, items=json.dumps(items), skipresults=skipresults))
        result:querys_pb2.InsertManyResponse = await protowrap.RPC(self, request)
        if(result == None or result.results == None): return None
        return json.loads(result.results)
    async def UpdateOne(self, item:dict, collectionname:str="entities"):
        request = base_pb2.Envelope(command="updateone")
        request.data.Pack(querys_pb2.UpdateOneRequest(
            collectionname=collectionname, item=json.dumps(item)))
        result:querys_pb2.UpdateOneResponse = await protowrap.RPC(self, request)
        if(result == None or result.result == None): return None
        return json.loads(result.result)
    async def UpdateDocument(self, query:dict, document:dict, collectionname:str="entities"):
        request = base_pb2.Envelope(command="updatedocument")
        request.data.Pack(querys_pb2.UpdateDocumentRequest(
            collectionname=collectionname, query=json.dumps(query), document=json.dumps(document)))
        result:querys_pb2.UpdateDocumentResponse = await protowrap.RPC(self, request)
        if(result == None or result.opresult == None): return None
        return result.opresult
    async def InsertOrUpdateOne(self, item:dict, collectionname:str="entities", uniqeness:str="_id"):
        request = base_pb2.Envelope(command="insertorupdateone")
        request.data.Pack(querys_pb2.InsertOrUpdateOneRequest(
            collectionname=collectionname, uniqeness=uniqeness, item=json.dumps(item)))
        result:querys_pb2.InsertOrUpdateOneResponse = await protowrap.RPC(self, request)
        if(result == None or result.result == None): return None
        return json.loads(result.result)
    async def InsertOrUpdateMany(self, items:dict, collectionname:str="entities", uniqeness:str="_id", skipresults:bool=False):
        request = base_pb2.Envelope(command="insertorupdatemany")
        request.data.Pack(querys_pb2.InsertOrUpdateManyRequest(
            collectionname=collectionname, uniqeness=uniqeness, items=json.dumps(items), skipresults=skipresults))
        result:querys_pb2.InsertOrUpdateManyResponse = await protowrap.RPC(self, request)
        if(result == None or result.results == None): return None
        return json.loads(result.results)
    async def DeleteOne(self, id:str, collectionname:str="entities", recursive:bool=False):
        request = base_pb2.Envelope(command="deleteone")
        request.data.Pack(querys_pb2.DeleteOneRequest(
            collectionname=collectionname, id=id, recursive=recursive))
        result:querys_pb2.DeleteOneResponse = await protowrap.RPC(self, request)
        if(result == None): return 0
        return result.affectedrows
    async def DeleteMany(self, query:dict, collectionname:str="entities", recursive:bool=False):
        request = base_pb2.Envelope(command="deletemany")
        request.data.Pack(querys_pb2.DeleteManyRequest(
            collectionname=collectionname, query=json.dumps(query), recursive=recursive))
        result:querys_pb2.DeleteManyResponse = await protowrap.RPC(self, request)
        if(result == None): return 0
        return result.affectedrows
    async def Watch(self, collectionname, paths:list, callback):
        request = base_pb2.Envelope(command="watch")
        request.data.Pack(watch_pb2.WatchRequest (collectionname=collectionname, paths=paths))
        result:watch_pb2.WatchResponse = await protowrap.RPC(self, request)
        self.watches[result.id] = callback
        return result.id
    async def UnWatch(self, id:str):
        request = base_pb2.Envelope(command="unwatch")
        request.data.Pack(watch_pb2.UnWatchRequest (id=id))
        result:watch_pb2.UnWatchResponse = await protowrap.RPC(self, request)
        self.watches.pop(id, None)
class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now = True
    sys.exit()
