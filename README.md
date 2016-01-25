# mongomanager
Distributed mongodb, you can access remote database as fast as local database.

這個是在pymongo的基礎上將mongodb包裝成一個分佈式的數據庫的程序，利用這個程序可以用像訪問本地數據庫一樣的速度去訪問遠程數據庫。

依賴的庫有tornado和futures，安裝完依賴程序就可以執行了。

啓動控制節點

    python manager.py

控制節點的監聽端口可以在manager.py最下面進行更改。

客戶端程序示例

    from node import Node # 引入客戶端節點庫
    import time
    import random

    if __name__ == "__main__":
        mNode = Node("ws://[manager node address]", "[database name]") # 創建鏈接
        mNode.start() # 啓動節點
        count = 0
        while True:
            count += 1
            if count > 100:
                break
            time.sleep(random.random()/100)
            mNode.insert_one({"name": "test" + str(count), "time": time.time()}) # 隨機向數據庫插入數據
        while True:
            time.sleep(1)


## 原理
程序由兩個基本的部分組成。控制節點和客戶端節點，對應着manager.py和node.py這兩個文件。
所有的客戶端節點都和控制節點相連。控制節點選取其中一個客戶端節點作爲主節點。主節點保存着最新的數據庫數據。如果某一個客戶端節點想要更改數據庫，那麼它就要先向控制節點申請成爲主節點。控制節點把這個節點和當前主節點進行比較。如果此節點已經同步到最新數據就把當前主節點設置成這個節點。成爲主節點後這個節點就可以進行數據庫操作了。此時所有的數據庫更改操作都會被推送到其他節點，由控制節點向其他節點進行廣播。其他節點收到請求後執行相應的數據庫操作。由此保證各個節點的數據同步。
