# -*- coding: utf-8 -*-
from threading import Thread
from queue import Queue
from queue import Empty

class ThreadManager(object):
    '''线程池管理类'''
    def __init__(self, threadNum):
        super(ThreadManager, self).__init__()
        if threadNum > 0:
            self.threadNum = threadNum
        else:
            self.threadNum = 10
        self.isRun = True
        # 存储线程
        self.threadPool = []
        # 待线程处理任务队列
        self.threadTaskQueue = Queue()
        self.initThreadPool()

    def initThreadPool(self):
        '''初始化线程'''
        for i in range(self.threadNum):
            self.threadPool.append(ThreadProcessor(self))

    def addTask(self, func, *args, **kwargs):
        '''添加线程任务'''
        self.threadTaskQueue.put((func, args, kwargs))

    def stopThreads(self, threadNum):
        '''停止指定个数线程'''
        if threadNum < 1:
            return None
        for item in self.threadPool[0:min(threadNum, self.threadNum)]:
            item.state = False

    def addThreads(self, threadNum):
        '''添加指定个数线程'''
        if threadNum > 0:
            for i in range(threadNum):
                self.threadPool.append(ThreadProcessor(self))

    def isAllThreadNoTask(self):
        '''判断是否所有线程都在空跑'''
        for item in self.threadPool:
            if item.noTaskRunTimes < 7:
                return False
        return True


class ThreadProcessor(Thread):
    '''线程池线程类'''
    def __init__(self, threadManager):
        Thread.__init__(self)
        self.setDaemon(True)
        self._threadManager = threadManager
        self.state = True
        self.noTaskRunTimes = 0
        self.start()

    def run(self):
        while self.state and self._threadManager.isRun:
            try:
                func, args, kwargs = self._threadManager.threadTaskQueue.get(timeout = 1)
                self.noTaskRunTimes = 0
            except Empty:
                if self.noTaskRunTimes < 10:
                    self.noTaskRunTimes += 1
                continue

            func(*args, **kwargs)
