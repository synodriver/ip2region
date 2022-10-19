#-*- coding:utf-8 -*-
"""
" ip2region python seacher client module benchmark test file
"
" Autho: koma<komazhang@foxmail.com>
" Date : 2018-10-04
"""
import threading
import time, sys

from ip2Region import Ip2Region

class BenchmarkThread(threading.Thread):
    __searcher = None
    __lock = None

    def __init__(self, searcher, lock):
        self.__searcher = searcher
        self.__lock = lock
        threading.Thread.__init__(self)

    def run(self):
        self.__lock.acquire()
        try:
            sTime = time.time() * 1000
            data = self.__searcher.memorySearch("49.220.138.233")
            eTime = time.time() * 1000
            # @Note uncomment the print to make it more like the product environment
            print("%s|%s in %5f millseconds" % (data["city_id"], data["region"].decode('utf-8'), eTime - sTime))
        finally:
            self.__lock.release()

if __name__ == "__main__":
    dbFile = sys.argv[1] if ( len(sys.argv) > 2 ) else "./data/ip2region.db"
    threads = []
    searcher = Ip2Region(dbFile)
    lock = threading.Lock()

    for _ in range(10000):
        t = BenchmarkThread(searcher, lock)
        threads.append(t)

    sTime = time.time() * 1000
    for t in threads:
        t.start()

    for t in threads:
        t.join()
    eTime = time.time() * 1000

    print("Benchmark done: %5f" % (eTime - sTime))
