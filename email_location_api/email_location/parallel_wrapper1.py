__author__ = 'joswin'

import sys,time
import threading
from email_location_finder import EmailLocationFinder

loc_finder = EmailLocationFinder()

def get_linkedin_out1(args_dict):
	return loc_finder.get_location_ddg_linkedin_dictinput(args_dict)

def get_angellist_out1(args_dict):
	return loc_finder.get_location_angellist_dictinput(args_dict)

class Worker():
    ''' Need work.
    '''
    def __init__(self):
        pass

    def linkedin_result_best(self,args_dict,event):
        out_dic = loc_finder.get_location_ddg_linkedin_dictinput(args_dict)
        if out_dic['Location']:
            self.out.append(out_dic)
        event.set()

    def angellist_result_best(self,args_dict,event):
        out_dic = loc_finder.get_location_angellist_dictinput(args_dict)
        if out_dic['Location']:
            self.out.append(out_dic)
        event.set()

    def find_location_best(self,args_dict):
        '''
        :param args_dict:
        :return:
        '''
        self.out = []
        timeout, n_threads = 10, 2
        final_dic = {'Location':'','Confidence':0}
        event1,event2 = threading.Event(),threading.Event()
        t1 = threading.Thread(target=self.linkedin_result_best, args=(args_dict,event1))
        t2 = threading.Thread(target=self.angellist_result_best, args=(args_dict,event2))
        t1.daemon = True
        t2.daemon = True
        t1.start()
        t2.start()
        start = time.time()
        event1.wait(timeout=timeout)
        new_timeout = timeout - (time.time()-start)
        if new_timeout>0:
            event2.wait(timeout=new_timeout)
        if self.out:
            for out_dic in self.out:
                if out_dic['Confidence']>final_dic['Confidence']:
                    final_dic['Location'] = out_dic['Location']
                    final_dic['Confidence'] = out_dic['Confidence']
        print(self.out)
        return final_dic

    def linkedin_result_fast(self,args_dict,event):
        out_dic = loc_finder.get_location_ddg_linkedin_dictinput(args_dict)
        if out_dic['Location']:
            self.out.append(out_dic)
            event.set()

    def angellist_result_fast(self,args_dict,event):
        out_dic = loc_finder.get_location_angellist_dictinput(args_dict)
        if out_dic['Location']:
            self.out.append(out_dic)
            event.set()

    def find_location_fast(self,args_dict):
        '''
        :param args_dict:
        :return:
        '''
        self.out = []
        final_dic = {'Location':'','Confidence':0}
        event = threading.Event()
        t1 = threading.Thread(target=self.linkedin_result_fast, args=(args_dict,event,))
        t2 = threading.Thread(target=self.angellist_result_fast, args=(args_dict,event,))
        t1.daemon = True
        t2.daemon = True
        t1.start()
        t2.start()
        event.wait(timeout=10)
        if self.out:
            return self.out[0]
        return final_dic


if __name__ == '__main__':
    w = Worker()
    if len(sys.argv) == 2:
        n_process = int(sys.argv[1])
        res = w.find_location_best({'Name':'Danny Forest','Company':'Massive Damage'},n_process)
    else:
        res = w.find_location_best({'Name':'Danny Forest','Company':'Massive Damage'})
    print(res)