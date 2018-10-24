#!/usr/bin/env python
# coding=utf-8
import time
import random

from queue import Queue
from threading import Lock
from threading import Thread

class DataLoader:

    def __init__(self, filename, func, num_epoch, batch_size, buffer_size, shuffle=False, num_worker=1):
        """
            func: consume a line and return a dict of arrays
        """
        self.filename = filename
        self.num_epoch = num_epoch
        self.batch_size = batch_size
        self.buffer_size = buffer_size
        self.shuffle = shuffle
        self.num_worker = num_worker
        
        self.cur_epoch = 0
        self.queue = Queue(buffer_size)
        self.lock = Lock()
        self.file = open(filename, 'r')
        self.rest = 0
        
        self.workers = []

        for i in range(num_worker):
            worker = Thread(target=self.make_func(func), args=())
            worker.start()

    def make_func(self, func):
        def target_func():
            while True:
                self.lock.acquire()
                if self.cur_epoch >= self.num_epoch:
                    self.lock.release()
                    break
                line = self.file.readline().strip()
                if line == '':
                    self.cur_epoch += 1
                    if self.cur_epoch >= self.num_epoch:
                        self.lock.release()
                        break
                    self.file.seek(0)
                    line = self.file.readline().strip()
                self.rest += 1
                self.lock.release()
                processed = func(line)
                self.queue.put(processed)
        return target_func



    def iterator(self):
        buff = []
        data = []
        while True:
            if self.rest == 0 and self.queue.empty() and len(buff) == 0:
                break
            while not self.queue.empty() and len(buff) < self.buffer_size:
                buff.append(self.queue.get())
                self.queue.task_done()
            
            if len(buff) > 0:
                idx = int(len(buff)*random.random())
                data.append(buff.pop(idx))
                if len(data) == self.batch_size or len(buff) == 0:
                    print('consume', data)
                    yield data
                    self.rest -= len(data)
                    data = []
        


def func(line):
    time.sleep(0.05)
    return int(line)


dataloader = DataLoader('test.txt', func, 2, 4, 20, num_worker=20)
iterator = dataloader.iterator()
count = dict()

start = time.time()
for i in range(100):
    try:
        data = next(iterator)
    except StopIteration:
        break
    for v in data:
        if v in count:
            count[v] += 1
        else:
            count[v] = 1
print(time.time() - start)
print(count, sum(count.values()))
