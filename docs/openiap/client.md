Module openiap.client
=====================

Classes
-------

`Client(url: str = '', grpc_max_receive_message_length: int = 4194304)`
:   

    ### Methods

    `Aggregate(self, aggregates: dict = {}, collectionname: str = 'entities', queryas: str = None)`
    :

    `Close(self)`
    :

    `Count(self, query: dict = {}, collectionname: str = 'entities', queryas: str = None)`
    :

    `DeleteMany(self, query: dict, collectionname: str = 'entities', recursive: bool = False)`
    :

    `DeleteOne(self, id: str, collectionname: str = 'entities', recursive: bool = False)`
    :

    `DownloadFile(self, Id: str = None, Filename: str = None)`
    :

    `DropCollection(self, collectionname: str)`
    :

    `GetDocumentVersion(self, id: str, collectionname: str, version: int = 0, decrypt: bool = True)`
    :

    `GetElement(self, xpath: str)`
    :

    `InsertMany(self, items: [], collectionname: str = 'entities', skipresults: bool = False)`
    :

    `InsertOne(self, item: dict, collectionname: str = 'entities')`
    :

    `InsertOrUpdateMany(self, items: dict, collectionname: str = 'entities', uniqeness: str = '_id', skipresults: bool = False)`
    :

    `InsertOrUpdateOne(self, item: dict, collectionname: str = 'entities', uniqeness: str = '_id')`
    :

    `ListCollections(self, includehist: bool = False)`
    :

    `PopWorkitem(self, wiq: str, includefiles: bool = False, compressed: bool = False)`
    :

    `PushWorkitem(self, wiq: str, name: str, payload: dict, files: <built-in function any> = None, wiqid: str = None, nextrun: datetime.datetime = None, priority: int = 2, compressed: bool = False)`
    :

    `Query(self, collectionname: str = 'entities', query: dict = {}, projection: dict = {}, top: int = 100, skip: int = 0, orderby=None, queryas: str = None)`
    :

    `QueueMessage(self, queuename: str, payload, correlationId=None, striptoken=True, rpc=False)`
    :

    `RegisterQueue(self, queuename: str, callback)`
    :

    `Signin(self, username: str = None, password: str = None, ping: bool = True, validateonly: bool = False, longtoken: bool = False)`
    :

    `UnWatch(self, id: str)`
    :

    `UpdateDocument(self, query: dict, document: dict, collectionname: str = 'entities')`
    :

    `UpdateOne(self, item: dict, collectionname: str = 'entities')`
    :

    `UpdateWorkitem(self, workitem, files: <built-in function any> = None, compressed: bool = False, ignoremaxretries: bool = False)`
    :

    `Watch(self, collectionname, paths: list, callback)`
    :

    `ainput(self, string: str) ‑> str`
    :

    `onconnected(self, client)`
    :

    `onmessage(self, client, command, rid, message)`
    :

`GracefulKiller()`
:   

    ### Class variables

    `kill_now`
    :

    ### Methods

    `exit_gracefully(self, *args)`
    :