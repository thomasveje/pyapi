Module openiap.protowrap
========================

Classes
-------

`protowrap()`
:   

    ### Class variables

    `pending: dict`
    :

    `streams: dict`
    :

    ### Static methods

    `Connect(client)`
    :

    `DownloadFile(client, Id: str = None, Filename: str = None)`
    :

    `RPC(client, request: base_pb2.Envelope, id: str = '')`
    :

    `SetStream(rid: str)`
    :

    `runws(client)`
    :

    `sendMesssag(client, request: base_pb2.Envelope, id: str)`
    :

    `timestamp()`
    :

    `uniqueid()`
    :

    ### Methods

    `Unpack(self, message: base_pb2.Envelope)`
    :

    `parse_message(client, message: base_pb2.Envelope)`
    :