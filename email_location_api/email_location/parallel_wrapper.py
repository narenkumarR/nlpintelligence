__author__ = 'joswin'

import sys
import multiprocessing
from email_location_finder import EmailLocationFinder

loc_finder = EmailLocationFinder()

def get_linkedin_out1(args_dict):
	return loc_finder.get_location_ddg_linkedin_dictinput(args_dict)

def get_angellist_out1(args_dict):
	return loc_finder.get_location_angellist_dictinput(args_dict)

class Worker():
    ''' Need work. Not sure if processes are closed etc
    '''
    def __init__(self):
        pass
        # self.queue = multiprocessing.Manager().Queue()
    def log_result(self,out_dic):
        self.out.append(out_dic)
        self.event.set()
    def do_job(self,args_dict,n_process=2):
        # try:
        self.out = []
        self.pool = multiprocessing.Pool(processes=n_process)
        self.manager = multiprocessing.Manager()
        self.event = self.manager.Event()
        self.pool.apply_async(get_linkedin_out1, args=(args_dict,), callback=self.log_result)
        self.pool.apply_async(get_angellist_out1, args=(args_dict,), callback=self.log_result)
        # self.pool.daemon = True
        self.pool.close()
        # self.pool.join()
        # self.pool.terminate()
        self.event.wait()
        self.pool.terminate()
        return self.out
        # except:
        #     return self.out
    def find_best_location(self,args_dict,n_process=2):
        self.do_job(args_dict,n_process)
        final_dic = {'Location':'','Confidence':0}
        for out_dic in self.out:
            if out_dic['Confidence']>final_dic['Confidence']:
                final_dic['Location'] = out_dic['Location']
                final_dic['Confidence'] = out_dic['Confidence']
        return final_dic

if __name__ == '__main__':
    w = Worker()
    if len(sys.argv) == 2:
        n_process = int(sys.argv[1])
        res = w.find_best_location({'Name':'Danny Forest','Company':'Massive Damage'},n_process)
    else:
        res = w.find_best_location({'Name':'Danny Forest','Company':'Massive Damage'})
    print(res)