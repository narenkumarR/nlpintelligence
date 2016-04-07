__author__ = 'joswin'

import Queue
import threading

from email_location_finder import EmailLocationFinder
loc_finder = EmailLocationFinder()

def get_linkedin_out(q,args_dict):
	q.put(loc_finder.get_location_ddg_linkedin_dictinput(args_dict))

def get_angellist_out(q,args_dict):
	q.put(loc_finder.get_location_angellist_dictinput(args_dict))

def get_out(inp_dict = {'Name':'Mo Yehia','Company':'Sidewalk'}):
    '''
    :param inp_dict:
    :return:
    '''
    q = Queue.Queue()
    t = threading.Thread(target=get_linkedin_out, args = (q,inp_dict))
    t.daemon = True
    t.start()
    t = threading.Thread(target=get_angellist_out, args = (q,inp_dict))
    t.daemon = True
    t.start()
    s = q.get()
    return s


from multiprocessing import Pool
import Queue

from email_location_finder import EmailLocationFinder

loc_finder = EmailLocationFinder()
def get_linkedin_out(q,args_dict):
	q.put(loc_finder.get_location_ddg_linkedin_dictinput(args_dict))

def get_angellist_out(q,args_dict):
	q.put(loc_finder.get_location_angellist_dictinput(args_dict))

def get_linkedin_out1(args_dict):
	return loc_finder.get_location_ddg_linkedin_dictinput(args_dict)

def get_angellist_out1(args_dict):
	return loc_finder.get_location_angellist_dictinput(args_dict)

class Worker():
    def __init__(self):
        pass
        # self.queue = multiprocessing.Manager().Queue()
    def log_result(self,out_dic):
        self.out.append(out_dic)
        self.pool.terminate()
    def do_job(self,args_dict):
        self.out = []
        self.pool = Pool(processes=2)
        self.pool.apply_async(get_linkedin_out1, args=(args_dict,), callback=self.log_result)
        self.pool.apply_async(get_angellist_out1, args=(args_dict,), callback=self.log_result)
        self.pool.close()
        self.pool.join()
        return self.out
    # def do_job_main(self,args_dict):
    #     self.do_job(args_dict)
    #     return self.out


w = Worker()
res = w.do_job({'Name':'Mo Yehia','Company':'Sidewalk'})


import multiprocessing as mp

from email_location_finder import EmailLocationFinder

loc_finder = EmailLocationFinder()

def get_linkedin_out1(args_dict):
	return loc_finder.get_location_ddg_linkedin_dictinput(args_dict)

def get_angellist_out1(args_dict):
	return loc_finder.get_location_angellist_dictinput(args_dict)

out = []
def log_result(out_dic):
    out.append(out_dic)

pool = mp.Pool()
args_dict1 = {'Name':'Mo Yehia','Company':'Sidewalk'}
pool.apply_async(get_linkedin_out1, args=(args_dict1,), callback=log_result)
pool.apply_async(get_angellist_out1, args=(args_dict1,), callback=log_result)
pool.close()
pool.join()

print out
