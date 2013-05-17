import time, sys, uuid, json, random, uuid
from ws4py.client.threadedclient import WebSocketClient

class ReactiveDict(dict):
    def __init__(self, pyDict, myCollection):
        self.myCollection = myCollection
        self.nonReactiveUpdate(pyDict)
        self.reactive = True

    def __setitem__(self, item, setTo):
        # If no actual change happens, do nothing!
        if (item in self) and (self[item] == setTo): return

        # Actual local change happened. First update locally
        self.nonReactiveSet(item, setTo)
        self.pushUpdate()

    def pushUpdate(self):
        # If reactive, syncronize with using ddpClient
        if self.reactive:
            resultID = self.myCollection.client.method( "/" + self.myCollection.collectionName + "/update", [{"_id":self["_id"]}, self])

    def update(self, pyDict):
        # Overriding built in update this way
        # makes the reactive stuff work properly
        super(ReactiveDict, self).update(pyDict)
        self.pushUpdate()

    # Non reactive versions of set and update
    def nonReactiveUpdate(self, pyDict):
        for k in pyDict.keys():
            self.nonReactiveSet(k, pyDict[k])
    def nonReactiveSet(self, item, setTo):
        super(ReactiveDict, self).__setitem__(item, setTo)

    def setReactive(self, enabled):
        self.reactive = enabled


class DDPCollection(list):
    def __init__(self, collectionName, ddpClient):
        self.collectionName = collectionName
        self.client = ddpClient
        self._ids = []

    #########################
    # User facing functions #
    #########################
    def insert(self, pyDict):
        # Generate an id if id dosent already have one
        if "_id" not in pyDict:
            pyDict["_id"] = uuid.uuid4().hex

        # Call the apropriate update method
        methodName = "/" + self.collectionName + "/insert"
        self.client.method(methodName, [pyDict])
        # MRG TODO:

    def find(self, selector):
        results = []
        for doc in self:
            for k, v in selector.items():
                if (k in doc) and (doc[k] == v):
                    results.append(doc)
        return results

    def findOne(self, selector):
        results = []
        for doc in self:
            for k, v in selector.items():
                if (k in doc) and (doc[k] == v):
                    return doc
        return None

    def remove(self, selector):
        docsToRemove = find(selector)
        for doc in docsToRemove:
            self._ids.remove(doc["_id"])
            self.remove(doc)

            # Remove them in the remote collection as well
            methodName = "/" + self.collectionName + "/remove"
            self.client.method(methodName, [doc])

    def __getitem__(self, offset):
        return super(DDPCollection, self).__getitem__(offset)

    #######################################
    # Processed calles from meteor server #
    #######################################
    def _added(self, id, obj={}):
        # Put the id in the object, and make it reactive
        obj["_id"] = id
        obj = ReactiveDict(obj, self)

        # Then add the object and its id to the respective lists
        self._ids.append(id)
        self.append(obj)

    def _changed(self, id, obj, cleared):
        # Update new/changed fields
        itemToBeUpdated = self._getByID(id)
        for k in obj.keys(): itemToBeUpdated.nonReactiveSet(k, obj[k])
        # Remove cleared ones
        for k in cleared: itemToBeUpdated.pop(k)

    def _addedBefore(self, id, obj, idBefore):
        # Put the id in the object, and make it reactive
        obj["_id"] = id
        obj = ReactiveDict(obj, self)

        # Find the place that it goes
        beforeIndex = self._ids.index(itemID)

        # Put it there in both lists
        self._ids.insert(beforeIndex, id)
        self.insert(beforeIndex, obj)

    def _movedBefore(self, id, idBefore):
        # Remove the object  . . .
        obj = self.removed(id)
        # then stick it the new place it goes.
        self.addedBefore(id, obj, idBefore)

    def _removed(self, id):
        # Remove the object
        oldIndx = self._ids.index(id)
        self._ids.pop(oldIndx)
        return self.pop(oldIndx)

    def _getByID(self, itemID):
        return super(DDPCollection, self).__getitem__(self._ids.index(itemID))


# javascripty access to collections
class CollectionCollection(object):
    def __getitem__(self, item):
        return getattr(self, item)

class DDPClient(WebSocketClient):
    """Simple wrapper around Websockets for DDP connections"""
    def __init__(self, url, debugPrint = False, raiseErrors = False, printErrors = True):
        WebSocketClient.__init__(self, url)
        self.debugPrint = debugPrint

        self.outstandingRequests = {}
        self.subNameToID = {}

        # Place to collect collections
        self.collections = CollectionCollection()

        self.raiseErrors = raiseErrors
        self.printErrors = printErrors

        self.DDP_Connected = False

        self.nrecieved = 0

    def connectDDP(self, timeout=5):
        self.connect()

        startTime = time.time()
        while time.time() - startTime < timeout:
            if self.DDP_Connected:
                return
            time.sleep(0.05)
        raise RuntimeError("DDP Connection failed. . . :'(")

    def _ddpErrorHandler(self, errorString):
        if self.raiseErrors:
            raise RuntimeError(errorString)

        if self.printErrors:
            print(errorString)

    ##############################
    # ws4py overridden functions #
    ##############################
    def opened(self):
        """Set the connect flag to true and send the DDP connect message to the server."""
        connectionMsg = {"msg": "connect", "version":"pre1", "support":["pre1"]}
        self._sendDict(connectionMsg)
    def closed(self, code, reason=None):
        """Called when the connection is closed"""
        print('* CONNECTION CLOSED {0} ({1})'.format(code, reason))
    def received_message(self, data):
        if self.debugPrint:
            print('RAW From Server: {}'.format(data))
            sys.stdout.flush()

        # MRG TODO: catch non-json errors
        # MRG TODO: add ejson support here
        msgDict = json.loads(str(data))

        if self.DDP_Connected:
            self._recieveDDP(msgDict)
        else:
            self._recieveConnect(msgDict)

    # Function to establish a DDP connection
    def _recieveConnect(self, data):
        # "The server may send an initial message which is a JSON object
        #  lacking a msg key. If so, the client should ignore it."
        if "msg" not in data: return

        if data["msg"] == "connected":
            self.DDP_Connected = True
            self.session = data["session"]
            return
        raise RuntimeError("DDP Connection Failed? :" + data)
        # MRG TODO: error states of this action

    # Function to process a DDP update received over ws
    def _recieveDDP(self, msgDict):
        """Parse an incoming message and print it. Also update
        self.pending appropriately"""
        assert "msg" in msgDict, "Malformed DDP response"
        msgType = msgDict['msg']

        handlerMethodName = "_handle_" + msgType
        # Make sure it is a method type we have a handler for
        if not hasattr(self, handlerMethodName):
            errMsg = "No handler for DDP message " + msgType
            print(errMsg)
            raise RuntimeError(errMsg)

        # Get the handler and pass the object to it
        handler = getattr(self, handlerMethodName)
        handler(msgDict)

    #########################
    # Convenience functions #
    #########################
    def _sendDict(self, msgDict):
        """Send a python dict formatted to json to the ws endpoint"""
        message = json.dumps(msgDict)
        if self.debugPrint:
            print("RAW To server:" + message)
        self.send(message)

    def _getCollection(self, collectionName):
        # Make a collection if necessary
        if not hasattr(self.collections, collectionName):
            setattr(self.collections, collectionName, DDPCollection(collectionName, self))
        # return the relevant collection
        return getattr(self.collections, collectionName)

    ###########################
    # DDP Message Type Handlers
    ###########################
    def _handle_error(self, msgDict):
        reasonString = msgDict.get("reason", "")
        detailString = msgDict.get("offendingMessage", "")

        errorString = 'DDP Error\n\t Reason:"{0}", Offending Message:"{1}"'.format( str(reasonString), str(detailString) )
        self._ddpErrorHandler(errorString)

    def _handle_nosub(self, msgDict):
        self._nosub_result_error_check(msgDict, "nosub")
        self.outstandingRequests[msgDict["id"]] = False

    def _handle_result(self, msgDict):
        self._nosub_result_error_check(msgDict, "result")
        self.outstandingRequests[msgDict["id"]] = msgDict.get("result", "{}")

    def _nosub_result_error_check(self, msgDict, source):
        # This function does nothing unless there is an error in the return json
        if not "error" in msgDict: return

        # Pull out the relevant fields
        err = msgDict["error"]
        errNumber  = err.get("error")
        errReason  = err.get("reason")
        errDetails = err.get("details")

        # Format and pass to the error handler
        errorMessage = "Error from {0}:{2} {3} ({1})".format(source, errNumber, errReason, errDetails)
        self._ddpErrorHandler(errorMessage)

    def _handle_ready(self, msgDict):
        for subscriptionID in msgDict["subs"]:
            self.outstandingRequests[subscriptionID] = True

    def _handle_added(self, msgDict):
        collection = self._getCollection(msgDict["collection"])
        collection._added(msgDict["id"], msgDict["fields"])

    def _handle_changed(self, msgDict):
        collection = self._getCollection(msgDict["collection"])
        collection._changed(msgDict["id"], msgDict.get("fields",{}), msgDict.get("cleared",{}))
    def _handle_removed(self, msgDict):
        collection = self._getCollection(msgDict["collection"])
        collection._removed(msgDict["id"])

    def _handle_addedBefore(self, msgDict):
        collection = self._getCollection(msgDict["collection"])
        collection._addedBefore(msgDict["id"], msgDict["fields"], msgDict["before"])

    def _handle_movedBefore(self, msgDict):
        collection = self._getCollection(msgDict["collection"])
        collection._movedBefore(msgDict["id"], msgDict["fields"], msgDict["before"])

    def _handle_updated(self, msgDict):
        pass
        # print msgDict


    # Generate an id for DDP calls that come back with results (subscribe/method)
    def _generateOutstandingID(self):
        mid = "pendingRequest_" + str(uuid.uuid4())
        self.outstandingRequests[mid] = None
        return mid
    #########################
    # User Facing Functions #
    #########################
    def newCollection(self, collectionName):
        return self._getCollection(collectionName)

    def method(self, methodName, params):
        # Make a request ID, and set it to pending
        mid = self._generateOutstandingID()

        # Fire the request
        msgDict = {"msg":"method", "method":methodName, "params":params, "id":mid}
        self._sendDict(msgDict)
        return mid

    def subscribe(self, subscriptionName, params=[]):
        # Make a request ID, and set it to pending
        subscriptionMessageID = self._generateOutstandingID()

        # Keep some internal tabs on subscriptions
        self.subNameToID[subscriptionName] = subscriptionMessageID

        # Message to make subscription happen
        msgDict = {"msg":"sub", "name":subscriptionName, "id":subscriptionMessageID, "params":params}
        self._sendDict(msgDict)

        return subscriptionMessageID

    def unsubscribe(self, subscriptionName):
        subID = self.subNameToID[subscriptionName]
        msgDict = {"msg":"sub", "id":subID}
        self._sendDict(msgDict)

    def getResult(self, resultID):
        # Spin waiting for result to come in
        while self.outstandingRequests[resultID] is None:
            time.sleep(0.01)

        # Return the result
        return self.outstandingRequests[resultID]

    def __del__(self):
        self.close(reason="Its not you, its me.")


if __name__ == "__main__":
    ddpc = DDPClient("ws://127.0.0.1:3000/websocket", debugPrint=True)
    ddpc.connectDDP()

    for methodName in ["working1", "working2", "broken"]:
        result = ddpc.method(methodName, ["work work work"]);
        print ddpc.getResult(result)
        time.sleep(1)
    time.sleep(5)
    
    ddpc.close(reason="Its not you, its me.")

