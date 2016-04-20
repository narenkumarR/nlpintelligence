__author__ = 'joswin'

from Queue import Queue
import threading
import logging
import time
from random import randint

class ThreadTest(object):
    def __init__(self):
        pass

    def worker(self):
        def get_output(inp,res_1,event):
            res_1['result'] = inp*inp
            event.set()
        while self.run_queue:
            time.sleep(randint(1,2))
            res_1 = {}
            in_no = self.in_queue.get()
            print('input {}'.format(in_no))
            event = threading.Event()
            t1 = threading.Thread(target=get_output, args=(in_no,res_1,event,))
            t1.daemon = True
            t1.start()
            event.wait(timeout=10)
            if 'result' in res_1:
                res = res_1['result']
                self.out_queue.put(res)
            self.in_queue.task_done()

    def worker_print(self):
        while True:
            time.sleep(2)
            res = self.out_queue.get()
            print('output {}'.format(res))
            self.out_queue.task_done()

    def run(self,inp_list,n_threads=2):
        '''
        :param inp_list:
        :param out_loc:
        :return:
        '''
        print('started')
        self.run_queue = True
        self.in_queue = Queue(maxsize=0)
        self.out_queue = Queue(maxsize=0)
        for i in range(n_threads):
            worker = threading.Thread(target=self.worker)
            worker.setDaemon(True)
            worker.start()
        worker = threading.Thread(target=self.worker_print)
        worker.setDaemon(True)
        worker.start()
        for i in inp_list:
            self.in_queue.put(i)
        print('in_queue join')
        while not self.in_queue.empty():
            try:
                time.sleep(2)
            except:
                self.run_queue = False
                break
        print('out_queue join')
        self.out_queue.join()
        print('Finished')
