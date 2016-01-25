#!/usr/bin/env python

'''
Distribute mongodb manager, sync data between different mongodb client.
'''
import json
from tornado import websocket, web, ioloop
import time
import thread
import threading
import uuid
from bson import json_util
from tornado import gen, locks
from concurrent.futures import ThreadPoolExecutor
from tornado.locks import Condition

thread_pool = ThreadPoolExecutor(4)

# new node joined to network
def on_new_node(node):
    pass

def set_master_node(node):
    pass

def on_set_master_node_req(node):
    pass

def on_sync_req(node):
    pass


class Manager():

    def __init__(self):
        self.new_master_node_flag = False
        self.clients = []
        self.masterNode = None
        self.db_cache = None
        self.operation_cache = []
        self.db_time = 0

    @gen.coroutine
    def set_master_node(self, node, value):
        res = yield node.send_req({
            "method": "post",
            "url": "/master-node",
            "data": value
        })
        if res["status"] != "OK":
            print "set master node failed"

    @gen.coroutine
    def get_db(self):
        if self.db_cache != None:
            raise gen.Return(self.db_cache)
        if self.masterNode == None:
            raise gen.Return(None)
        res = yield self.masterNode.send_req({
            "method": "get",
            "url": "/db"
        })
        self.db_cache = res["data"]
        self.db_time = res["time"]
        raise gen.Return(res["data"])

    @gen.coroutine
    def get_operations(self, mtime):
        if mtime == 0:
            mtime = self.db_time
        if len(self.operation_cache) > 0 and self.operation_cache[-1]["time"] <= mtime:
            resList = []
            for operation in self.operation_cache:
                if operation["time"] > mtime:
                    resList.append(operation)
            raise gen.Return(resList)
        if self.masterNode == None:
            raise gen.Return([])
        res = yield self.masterNode.send_req({
            "method": "get",
            "url": "/operations",
            "time": mtime,
        })
        if res["data"] != None:
            self.operation_cache = res["data"]
        raise gen.Return(self.operation_cache)

    @gen.coroutine
    def get_new_master_node(self):
        if self.new_master_node_flag:
            raise gen.Return()
        if len(self.clients) == 0:
            self.masterNode = None
            raise gen.Return()
        self.new_master_node_flag = True
        self.db_cache = None
        self.operation_cache = []
        lastOperation = 0
        lastNode = None
        for client in self.clients:
            if client.operationTime > lastOperation:
                lastNode = client
                lastOperation = client.operationTime
        if lastNode != None:
            self.masterNode = lastNode
        # no operations found in these clients, use the first client as master node
        else:
            self.masterNode = self.clients[0]
        # set new master note
        for client in self.clients:
            print "set master node"
            if client != self.masterNode:
                yield self.set_master_node(client, False)
            else:
                yield self.set_master_node(client, True)
        self.new_master_node_flag = False

    @gen.coroutine
    def set_new_master_node(self, node):
        if self.masterNode == None:
            yield self.get_new_master_node()
        if node.operationTime >= self.masterNode.operationTime:
            self.masterNode = node
            for client in self.clients:
                if client != self.masterNode:
                    yield self.set_master_node(client, False)
                else:
                    yield self.set_master_node(client, True)
            raise gen.Return(True)
        else:
            raise gen.Return(False)

    @gen.coroutine
    def send_operations(self, node, operation):
        node.operationTime = operation["time"]
        res = yield node.send_req({
            "url": "/operations",
            "method": "post",
            "data": operation
        })
        if res["status"] != "OK":
            print "send operation failed"

    @gen.coroutine
    def send_new_operations(self, node, operation):
        self.operation_cache.insert(0, operation)
        node.operationTime = operation["time"]
        for client in self.clients:
            if client != node:
                yield self.send_operations(client, operation)



manager = Manager()

class SocketHandler(websocket.WebSocketHandler):

    def __init__(self, *args, **kwargs):
        super(SocketHandler, self).__init__(*args, **kwargs)
        self.operationTime = 0
        self.reqLocks = {}
        self.resList = {}
        self.send_msg_lock = locks.Lock()

    @gen.coroutine
    def on_message(self, message):
        print "message:"
        print message
        req = json.loads(message)
        if not self.reqLocks.has_key(req["uuid"]):
            if not req.has_key("method"):
                print "##########################"
                print req
            # requests from clients
            if req["method"] == "get" and req["url"] == "/db":
                self.operationTime = manager.db_time
                # get db from current master node
                db = yield manager.get_db()
                yield self.send_msg(json.dumps({
                    "uuid": req["uuid"],
                    "data": db,
                }, default=json_util.default, indent=4))
                raise gen.Return()

            if req["method"] == "get" and req["url"] == "/operations":
                # get operation from master node
                operations = yield manager.get_operations(self.operationTime)
                yield self.send_msg(json.dumps({
                    "uuid": req["uuid"],
                    "data": operations,
                }, default=json_util.default, indent=4))
                # update operationTime
                if len(operations) == 0:
                    self.operationTime = time.time()
                else:
                    self.operationTime = operations[0]["time"]
                raise gen.Return()

            if req["method"] == "post" and req["url"] == "/master-node":
                if (yield manager.set_new_master_node(self)):
                    yield self.send_msg(json.dumps({
                        "uuid": req["uuid"],
                        "status": "OK",
                    }))
                else:
                    yield self.send_msg(json.dumps({
                        "uuid": req["uuid"],
                        "status": "sync"
                    }))
                raise gen.Return()

            if req["method"] == "post" and req["url"] == "/operations":
                self.operationTime = req["data"]["time"]
                yield manager.send_new_operations(self, req["data"])
                yield self.send_msg(json.dumps({
                    "uuid": req["uuid"],
                    "status": "OK",
                }))
                raise gen.Return()

        else:
            self.resList[req["uuid"]] = req
            self.reqLocks[req["uuid"]].notify()

    @gen.coroutine
    def open(self):
        print 'connected'
        manager.clients.append(self)
        if manager.masterNode == None:
            print "start get new master"
            yield manager.get_new_master_node()
        print manager.clients

    @gen.coroutine
    def on_close(self):
        print 'closed'
        manager.clients.remove(self)
        if manager.masterNode == self:
            manager.masterNode = None
            thread.start_new_thread(manager.get_new_master_node, ())
        print manager.clients

    @gen.coroutine
    def send_req(self, req):
        req["uuid"] = str(uuid.uuid4())
        self.send_msg(json.dumps(req, default=json_util.default, indent=4))
        self.reqLocks[req["uuid"]] = Condition()
        yield self.reqLocks[req["uuid"]].wait()
        res = self.resList[req["uuid"]]
        # delete record
        self.reqLocks.pop(req["uuid"], None)
        self.resList.pop(req["uuid"], None)
        raise gen.Return(res)

    @gen.coroutine
    def send_msg(self, message):
        with (yield self.send_msg_lock.acquire()):
            print "send"
            print message
            yield self.write_message(message)

app = web.Application([
    (r'/', SocketHandler)
])

if __name__ == '__main__':
    app.listen(8000)
    ioloop.IOLoop.instance().start()
