import os, sys, re, datetime
import json
import traceback
import asyncio
import logging
import zlib
import openiap
from google.protobuf import any_pb2
from openiap.client import GracefulKiller
from openiap.protowrap import protowrap 
from proto import base_pb2, queues_pb2
from google.protobuf import json_format

watchid = ""
async def onmessage(client, command, rid, message):
    reply = base_pb2.Envelope(command=command)
    reply.rid = rid
    try:
        if(command == "getelement"):
            logging.info(f"Server sent getelement {message.xpath}")
            reply.data.Pack(base_pb2.getelement(xpath=f"Did you say {message.xpath} ?"))
        elif(command == "queueevent"):
            data = json.loads(message.data);
        elif(command == "error"):
            reply.command = "noop"
            logging.error(f"SERVER ERROR !!! {str(message.message)} \n {str(message.stack)}")
        else:
            reply.command = "error"
            reply.data.Pack(base_pb2.error(message=f"Unknown command {command}"))
            logging.error(f"Got message with unknown command {command}")
    except Exception as e:
        print("runit EXCEPTION!!!!")
        print(repr(e))
        traceback.print_tb(e.__traceback__)
    return reply
async def findme333(client: openiap.Client, msg: base_pb2.Envelope, payload: dict):
    if("name" in payload):
        logging.info(f"findme333: {str(payload['name'])}")
    else:
        logging.info(f"findme333: triggered")
async def findme222(client: openiap.Client, msg: base_pb2.Envelope, payload: dict):
    if("name" in payload):
        logging.info(f"findme222: {str(payload['name'])}")
    elif "payload" in payload and "name" in payload["payload"]:
        logging.info(f"findme222: {str(payload['payload']['name'])}")
    else:
        logging.info(f"findme222: triggered")
async def pyqueue(client: openiap.Client, msg: base_pb2.Envelope, payload: dict):
    print("pyqueue triggered, PopWorkitem")
    # workitem = asyncio.run(self.c.PopWorkitem("pyqueue", True, True))
    workitem = await client.PopWorkitem("pyqueue", True, True)
    print("pyqueue PopWorkitem completed")
    # workitem.state = "successful"
    workitem.state = "retry"
    workitem = await client.UpdateWorkitem(workitem, ignoremaxretries=True)

    try:
        logging.info(f"findme222: {str(payload['name'])}")
    except:
        logging.info(f"findme222: {str(payload['payload']['name'])}")
        pass
async def getnoun(client: openiap.Client, msg: base_pb2.Envelope, payload: dict):
    text = "no-text"
    if("text" in payload):
        text = payload["text"]
    tmp = await client.GetElement("test")
    print(tmp)
    logging.info(f"getnoun: {str(msg.correlationId)} {str(text)}")
    payload["nouns"] = "yes, no"
    return payload

    # if(msg.replyto != "" and msg.replyto != None):
    #     reply = base_pb2.Envelope(command="queuemessage")
    #     res = json.dumps(payload)
    #     reply.data.Pack(queues_pb2.queuemessage(queuename=msg.replyto, data=res, striptoken=True, correlationId=msg.correlationId))
    #     client.queue.put(reply)
    # return payload
def timestamp():
        now = datetime.datetime.now()
        return  now.strftime("[%H:%M:%S] ")
async def onconnected(client: openiap.Client):
    try:
        user = await client.Signin()
        print(f"{timestamp()} Signed in as {user.name}")
        q = await client.RegisterQueue("findme333", findme333)
        print(f"{timestamp()} Registered queue {q}")
        q = await client.RegisterQueue("findme222", findme222)
        print(f"{timestamp()} Registered queue {q}")
        q = await client.RegisterQueue("pyqueue", pyqueue)
        print(f"{timestamp()} Registered queue {q}")
        q = await client.RegisterQueue("getnoun", getnoun)
        print(f"{timestamp()} Registered queue {q}")
        
    except (Exception,BaseException) as e:
        print("onconnected!!!!")
        print(repr(e))
        traceback.print_tb(e.__traceback__)
        pass
async def entitieswatch(client: openiap.client, operation:str, document: dict):
    print(f"{operation} {document['_id']} {document['name']}")
async def main():
    loglevel = os.environ.get("loglevel", logging.INFO)
    if loglevel==logging.INFO:
        logging.basicConfig(format="%(message)s", level=loglevel)
    else:
        logging.basicConfig(format="%(levelname)s:%(message)s", level=loglevel)
    apiurl = os.environ.get("apiurl", "")
    if(len(sys.argv) > 1): 
        apiurl = str(sys.argv[1])
        print(f"apiurl: {apiurl}")
    if(apiurl == ""): apiurl = os.environ.get("grpcapiurl", "")
    if(apiurl == ""): apiurl = os.environ.get("wsapiurl", "")
    if(apiurl == ""):
        sys.exit(f"apiurl missing")
    c = openiap.Client(apiurl)
    c.onmessage = onmessage
    c.onconnected = onconnected

    while True:
        try:
            # text = input("COMMAND: ")
            text = await c.ainput("COMMAND: ")
            text = re.sub(r'[^a-zA-Z0-9]', '', text)
            print(f"PROCESSING {text}")
            if text == "f":
                # filename = "/home/allan/Pictures/allan.png"
                id = "63d66fe01465b11939cd0d2d"
                name = "download.png";
                await c.DownloadFile(Id=id)
            elif text == "l":
                results = await c.ListCollections()
                if(results!=None):
                    for item in results:
                        print(item["name"])
            elif text == "g":
                result = await c.GetDocumentVersion("users", "6242d68a73057b27d277be88")
                if(result!=None):
                    print(result["name"])
            elif text == "c":
                result = await c.Count()
                print(result)
                result = await c.Count(collectionname="users")
                print(result)
            elif text == "a":
                results = await c.Aggregate("users", aggregates={"$match": {"_type": "user"}})
                if(results!=None):
                    for item in results:
                        print(item["name"])
            elif text == "i":
                result = await c.InsertOne(item={"name":"py find me"})
                print(f'{result["_id"]} {result["name"]}')
            elif text == "ii":
                results = await c.InsertMany(items=[{"name":"py find me 1"}, {"name":"py find me 2"}])
                if(results!=None):
                    for item in results:
                        print(f'{item["_id"]} {item["name"]}')
            elif text == "u":
                result = await c.InsertOne(item={"name":"py find me"})
                print(f'{result["_id"]} {result["name"]}')
                result["name"] = result["name"] + " Updated"
                result = await c.UpdateOne(item=result)
                print(f'{result["_id"]} {result["name"]}')
            elif text == "uu":
                result = await c.UpdateDocument({"_type":"unknown"},{"$set": {"name": "Updated"}} )
                print(f'acknowledged: {result.acknowledged} modified: {result.modifiedCount} matched: {result.matchedCount}')
            elif text == "iu":
                id = str(next(protowrap.uniqueid()))
                result = await c.InsertOrUpdateOne(item={"name":"InsertOrUpdateOne test " + id, "blah":"blah"}, uniqeness="blah")
                print(f'{result["_id"]} {result["name"]}')
            elif text == "ium":
                id = str(next(protowrap.uniqueid()))
                results = await c.InsertOrUpdateMany(items=[{"name":"InsertOrUpdateOne test 1 " + id, "blah":"blah1"}, {"name":"InsertOrUpdateOne test 2 " + id, "blah":"blah2"}], uniqeness="blah")
                if(results!=None):
                    for item in results:
                        print(f'{item["_id"]} {item["name"]}')
            elif text == "d":
                result = await c.InsertOne(item={"name":"py find me"})
                print(f'{result["_id"]} {result["name"]}')
                result["name"] = result["name"] + " Updated"
                result = await c.DeleteOne(result["_id"])
                print(f'Delete {result} items')
            elif text == "dd":
                result = await c.DeleteMany({"_type": "unknown"})
                print(f'Delete {result} items')
            elif text == "w":
                watchid = await c.Watch("entities", ["$.[?(@._type == 'test')]"], entitieswatch)
                print(f'watchid {watchid}')
            elif text == "uw":
                if(watchid!=""):
                    result = await c.UnWatch(id=watchid)
                    print(f'unwatch {watchid}')
                    watchid = ""
            elif text == "p":
                workitem = {"name": "Allan", "test":"Hi mom", "age":23, "files": []}
                filepath = "/home/allan/Pictures/allan.png"
                result = await c.PushWorkitem("q2", "find me", workitem, [filepath], compressed=True)
                logging.info(f"Workitem pushed with id {result._id}")

                workitem = await c.PopWorkitem("q2", True, True)
                if(workitem == None):
                    logging.info("No more workitems in queue q2")                    
                else:
                    try:
                        for f in workitem.files:
                            if f.compressed:
                                with open("download.png", "wb") as out_file:
                                    out_file.write(zlib.decompress(f.file))
                            else:
                                with open("download.png", "wb") as out_file:
                                    out_file.write(f.file)
                        logging.info(f"Popped workitem id {workitem._id}")
                        workitem.state = "successful"
                        payload = json.loads(workitem.payload)
                        logging.info(f"payload name {payload.get('name','unnamed')} workitem name {workitem.name}")
                        payload["name"] = "Allan 2222"
                        workitem.payload = json.dumps(payload)
                        workitem = await c.UpdateWorkitem(workitem, ["download.png"], True)
                        logging.info(f"Popped workitem id {workitem._id} now in state {workitem.state}")
                    except (Exception,BaseException) as e:
                         workitem.state = "retry"
                         workitem.errortype = "business" # business / application
                         workitem.errormessage = "".join(traceback.format_exception_only(type(e), e)).strip()
                         workitem.errorsource = "".join(traceback.format_exception(e))
                         await c.UpdateWorkitem(workitem)
                         print("Workitem EXCEPTION!!!!")
                         print(repr(e))
                         traceback.print_tb(e.__traceback__)
                         pass
            elif text == "s":
                signin = await c.Signin()
                logging.info(f"Signed in as {signin.name}")
            elif text == "q":
                # await c.QueueMessage("findme222", {"name":"py find me"})
                result = await c.QueueMessage("getnoun", {"name":"py find me"}, rpc=True)
                if(result!=None):
                    print(result["name"])
            elif text == "qq":
                # results = await c.Query(query={}, orderby={"name":-1})
                # results = await c.Query(query={}, orderby={"name":1})
                results = await c.Query()
                if(results!=None):
                    for item in results:
                        print(item["name"])
            elif text == "qqq":
                payload = {
                    "command": "invoke",
                    "workflowid": "5e0b52194f910e30ce9e3e49",
                    "data": {
                        "test": "please, did you find me ?"
                    }
                }
                result = await c.QueueMessage(queuename="5e0c8a75ea7ae0004e415e27", payload= payload, rpc=True)
                print("RESULT", result["data"])
            elif text == "pp":
                filepath = "/home/allan/Pictures/allan.png"
                for i in range(1, 5):
                    workitem = {"name": f"item {i}", "test":"Hi mom", "age":23}
                    result = await c.PushWorkitem("pyagent", f"item {i}", workitem, [filepath], compressed=True)
                    logging.info(f"Workitem pushed item {i} with id {result._id}")
                    result = await c.PushWorkitem("nodeagent", f"item {i}", workitem, [filepath], compressed=True)
                    logging.info(f"Workitem pushed item {i} with id {result._id}")
            elif text == "push":
                workitem = {"test":"Hi mom", "age":23}
                result = await c.PushWorkitem("q2", "find me", workitem)
                logging.info(f"Workitem pushed with id {result._id}")
            else:
                xpath = await c.GetElement(text)
                logging.info(xpath)
        except (Exception,BaseException) as e:
            print("MAIN EXCEPTION!!!!")
            print(repr(e))
            traceback.print_tb(e.__traceback__)
            pass
if __name__ == "__main__":
    killer = GracefulKiller()
    asyncio.run(main())
