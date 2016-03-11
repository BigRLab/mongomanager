'''
Distribute mongodb client node
'''

import json
import time
import websocket
import thread
import threading
import uuid
import pymongo
from pymongo import MongoClient
from bson import json_util


class Node():

    def __init__(self, address, dbname):
        self.sendLock = threading.Lock()
        self.sendReqLock = threading.Lock()
        self.responseLock = threading.Lock()
        self.ws_send_lock = threading.Lock()
        self.response = None
        self.reqid = None
        self.isMasterNode = False
        # db
        client = MongoClient()
        db = client[dbname]
        self.filesCollection = db["files"]
        self.operations = db["operations"]
        if self.operations.find().count() == 0:
            self.operationTime = 0
        else:
            self.operationsTime = self.operations.find().sort("time", pymongo.DESCENDING)[0]["time"]
        # start socket
        def on_message(ws, message):
            print "message:"
            print message
            res = json.loads(message)
            if res["uuid"] != self.reqid:
                # req from manager
                if res["url"] == "/master-node":
                    # manager update master node
                    self.isMasterNode = res["data"]
                    self.send_msg(json.dumps({
                        "uuid": res["uuid"],
                        "status": "OK",
                    }))
                    return
                if res["url"] == "/db" and res["method"] == "get":
                    self.send_msg(json.dumps({
                        "uuid": res["uuid"],
                        "data": list(self.filesCollection.find({},{"_id": False})),
                        "time": time.time(),
                        "status": "OK",
                    }, default=json_util.default,indent=4))
                    return
                if res["url"] == "/operations" and res["method"] == "get":
                    lastOperationTime = res["time"]
                    self.send_msg(json.dumps({
                        "uuid": res["uuid"],
                        "data": list(self.operations.find({"time": {"$gt": lastOperationTime}}, {"_id": False})
                            .sort("time", pymongo.DESCENDING) ),
                        "status": "OK",
                    }, default=json_util.default,indent=4))
                    return
                if res["url"] == "/operations" and res["method"] == "post":
                    self.operations.insert_one(res["data"])
                    self.operationTime = res["data"]["time"]
                    self.exec_operation(res["data"])
                    self.send_msg(json.dumps({
                        "uuid": res["uuid"],
                        "status": "OK",
                    }, default=json_util.default,indent=4))
                    return
            else:
                # res from server
                self.response = res
                self.responseLock.release()

        def on_error(ws, error):
            print error

        def on_close(ws):
            print "### closed ###"

        def on_open(ws):
            print "Open"
            pass
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(address, on_message=on_message, on_error=on_error, on_close=on_close)
        self.ws.on_open = on_open

    def ping(self):
        while True:
            time.sleep(30) # ping server every 30s
            res = self.send_req({
                "method": "get",
                "url": "/operations",
                "time": 0,
            })

    def start(self):
        thread.start_new_thread(self.ws.run_forever, ())
        time.sleep(5)
        thread.start_new_thread(self.ping, ())
        if self.isMasterNode:
            return
        self.sync_data()

    # database operations

    def clearDb(self):
        self.operations.drop()
        self.filesCollection.drop()

    def insert_one(self, *args, **kwargs):
        self.req_set_master()
        res = self.filesCollection.insert_one(*args, **kwargs)
        def send_insert():
            for arg in args:
                arg.pop("_id", None)
            newOperation = {
                "type": "insert_one",
                "record": json.dumps({
                    "args": args,
                    "kwargs": kwargs
                }),
                "time": time.time()
            }
            self.operations.insert_one(newOperation)
            newOperation.pop("_id", None)
            self.send_req({
                "method": "post",
                "url": "/operations",
                "data": newOperation
            })
        thread.start_new_thread(send_insert, ())
        return res

    def update_one(self, *args, **kwargs):
        self.req_set_master()
        res = self.filesCollection.update_one(*args, **kwargs)
        def send_update():
            newOperation = {
                "type": "update_one",
                "record": json.dumps({
                    "args": args,
                    "kwargs": kwargs
                }),
                "time": time.time()
            }
            self.operations.insert_one(newOperation)
            newOperation.pop("_id", None)
            self.send_req({
                "method": "post",
                "url": "/operations",
                "data": newOperation
            })
        thread.start_new_thread(send_update, ())
        return res

    def delete_one(self, *args, **kwargs):
        self.req_set_master()
        res = self.filesCollection.delete_one(*args, **kwargs)
        def send_delete():
            newOperation = {
                "type": "delete_one",
                "record": json.dumps({
                    "args": args,
                    "kwargs": kwargs
                }),
                "time": time.time()
            }
            self.operations.insert_one(newOperation)
            newOperation.pop("_id", None)
            self.send_req({
                "method": "post",
                "url": "/operations",
                "data": newOperation
            })
        thread.start_new_thread(send_delete, ())
        return res

    def find_one(self, *args, **kwargs):
        self.req_set_master()
        return self.filesCollection.find_one(*args, **kwargs)

    def find(self, *args, **kwargs):
        self.req_set_master()
        return self.filesCollection.find(*args, **kwargs)

    def count(self, *args, **kwargs):
        self.req_set_master()
        return self.filesCollection.count(*args, **kwargs)

    def req_operations(self, time):
        # get operations after the given time
        res = self.send_req({
            "method": "get",
            "url": "/operations",
            "time": time,
        })
        return res["data"]

    def req_db(self):
        res = self.send_req({
            "method": "get",
            "url": "/db",
        })
        return res["data"]

    def sync_data(self):
        # sync data with server
        # already has record, just get new operations
        if self.operations.count() > 0:
            lastOperation = self.operations.find({}, {"_id": False}).sort("time", pymongo.DESCENDING)[0]
            operations = self.req_operations(lastOperation["time"])
            if operations != None and len(operations) != 0:
                # in ascending order
                operations = operations[::-1]
                for operation in operations:
                    if operation["time"] > lastOperation["time"]:
                        self.operations.insert_one(operation)
                        self.exec_operation(operation)
        # new node, download db from manager
        else:
            if self.filesCollection.count() == 0:
                db = self.req_db()
                if db != None and len(db) != 0:
                    self.filesCollection.insert_many(db)
            operations = self.req_operations(0)
            if operations != None and len(operations) != 0:
                print operations
                operations = operations[::-1]
                for operation in operations:
                    self.operations.insert_one(operation)
                    self.exec_operation(operation)


    def req_set_master(self):
        if self.isMasterNode:
            return True
        operationTime = 0
        if self.operations.find().count() != 0:
            lastOperation = self.operations.find({}, {"_id": False}).sort("time", pymongo.DESCENDING)[0]
            print lastOperation
            operationTime = lastOperation["time"]
        res = self.send_req({
            "url": "/master-node",
            "method": "post",
            "time": operationTime
        })
        if res["status"] == "sync":
            self.sync_data()
            return self.req_set_master()
        if res["status"] == "OK":
            return True
        else:
            return False


    def send_req(self, req):
        self.sendReqLock.acquire()
        self.responseLock.acquire()
        # send req to manager server, block until response received
        req["uuid"] = str(uuid.uuid4())
        self.reqid = req["uuid"]
        self.sendLock.acquire()
        self.send_msg(json.dumps(req, default=json_util.default,indent=4))
        self.sendLock.release()
        # wait for response
        self.responseLock.acquire()
        self.responseLock.release()
        res = self.response
        self.response = None
        self.reqid = None
        self.sendReqLock.release()
        return res

    def exec_operation(self, operation):
        # exec db operations
        params = json.loads(operation["record"])
        if operation["type"] == "insert_one":
            return self.filesCollection.insert_one(*params["args"], **params["kwargs"])
        if operation["type"] == "update_one":
            return self.filesCollection.update_one(*params["args"], **params["kwargs"])
        if operation["type"] == "delete_one":
            return self.filesCollection.delete_one(*params["args"], **params["kwargs"])

    def send_msg(self, message):
        self.ws_send_lock.acquire()
        print "send:"
        print message
        self.ws.send(message)
        self.ws_send_lock.release()



if __name__ == "__main__":
    mNode = Node("ws://127.0.0.1:8000", "mnodedb")
    def test():
        while True:
            print "#####################"
            print mNode.isMasterNode
            time.sleep(4)
    #thread.start_new_thread(test, ())
    mNode.start()
    while True:
        time.sleep(1)
