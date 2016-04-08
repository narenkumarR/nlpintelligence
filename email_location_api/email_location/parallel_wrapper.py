__author__ = 'joswin'

import sys
import multiprocessing,threading
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
        self.manager = multiprocessing.Manager()
        # self.queue = multiprocessing.Manager().Queue()

        # except:
        #     return self.out

    def log_result_best(self,out_dic):
        self.out.append(out_dic)
        return True

    def do_job_best(self,args_dict,n_process=2):
        ''' Do not call this function directly
        :param args_dict:
        :param n_process:
        :return:
        '''
        self.pool = multiprocessing.Pool(processes=n_process)
        self.pool.apply_async(get_linkedin_out1, args=(args_dict,), callback=self.log_result_best)
        self.pool.apply_async(get_angellist_out1, args=(args_dict,), callback=self.log_result_best)

        # self.pool.daemon = True
        self.pool.close()
        self.pool.join()
        self.event.set()
        # self.pool.terminate()
        self.pool.terminate()
        return self.out

    def find_location_best(self,args_dict,n_process=2):
        ''' For finding location with highest confidence
        :param args_dict:
        :param n_process:
        :return:
        '''
        # import pdb
        # pdb.set_trace()
        self.out = []
        run_thread = threading.Thread(target=self.do_job_best,args= (args_dict,n_process))
        run_thread.start()
        self.event = self.manager.Event()
        if self.event.wait(timeout=15) or not run_thread.isAlive():
            # print(self.out)
            final_dic = {'Location':'','Confidence':0}
            for out_dic in self.out:
                if out_dic['Confidence']>final_dic['Confidence']:
                    final_dic['Location'] = out_dic['Location']
                    final_dic['Confidence'] = out_dic['Confidence']
            return final_dic

    def log_result_first(self,out_dic):
        # if not self.event.is_set():
        if out_dic['Location']:
            self.out.append(out_dic)
            self.event.set()
        # return True

    def do_job_first(self,args_dict,n_process=2):
        ''' Do not call this function directly
        :param args_dict:
        :param n_process:
        :return:
        '''
        # try:
        # self.event = self.manager.Event()
        self.pool = multiprocessing.Pool(processes=n_process)
        self.pool.apply_async(get_linkedin_out1, args=(args_dict,), callback=self.log_result_first)
        self.pool.apply_async(get_angellist_out1, args=(args_dict,), callback=self.log_result_first)
        # self.pool.daemon = True
        self.pool.close()
        self.pool.join()
        self.pool.terminate()

    def find_location_fast(self,args_dict,n_process=2):
        ''' For returning location the fastest
        :param args_dict:
        :param n_process:
        :return:
        '''
        self.out = []
        run_thread = threading.Thread(target=self.do_job_first,args= (args_dict,n_process))
        run_thread.start()
        self.event = self.manager.Event()
        while True:
            if self.out or self.event.wait(timeout=10):
                break
        if self.out:
            return self.out[0]
        else:
            return {'Location':'','Confidence':0}

if __name__ == '__main__':
    w = Worker()
    if len(sys.argv) == 2:
        n_process = int(sys.argv[1])
        res = w.find_location_best({'Name':'Danny Forest','Company':'Massive Damage'},n_process)
    else:
        res = w.find_location_best({'Name':'Danny Forest','Company':'Massive Damage'})
    print(res)