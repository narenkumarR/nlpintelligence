__author__ = 'joswin'

import re
import pdb
import csv
import pandas as pd
import codecs
import logging
class CsvReader(object):
    def __init__(self):
        pass

    def read_csv(self,f_name,n_cols=27,prob_cols = (19,22),chunk_size = 10000):
        '''
        :param file:
        :param n_cols:
        :param prob_cols : the columns with problem
        :chunk_size : size to be processed at a time
        :return:
        '''
        out_dic = {}
        ind = 0
        chunk_ind = 0
        actual_cols_no = prob_cols[1]-prob_cols[0]+1
        cols = []
        with open(f_name,'rU') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            # for line_list in reader:
            while True:
                try:
                    line_list = reader.next()
                except StopIteration:
                    break
                except :
                    logging.exception('the following error happened')
                    logging.info('Error at line no: {}'.format(chunk_ind*chunk_size+ind))
                    continue
                if not cols and ind == 0:
                    cols = line_list
                    if not n_cols:
                        n_cols = len(cols)
                    continue
                # line = re.sub('\r|\n','',line)
                # line_list = line.split(',')
                if len(line_list) == n_cols:
                    # new_list = line_list[:prob_cols[0]] + '|'.join(line_list[prob_cols[0]:prob_cols[1]+1])+\
                    #                 line_list[prob_cols[1]+1:]
                    actual_list = line_list
                elif len(line_list) > n_cols:
                    if prob_cols:
                        # pdb.set_trace()
                        # new_list = line_list[:prob_cols[0]] + '|'.join(line_list[prob_cols[0]:prob_cols[1]+1+diff_len])+\
                        #                 line_list[prob_cols[1]+1+diff_len:]
                        diff_len = len(line_list)-n_cols
                        probl_list = line_list[prob_cols[0]:prob_cols[1]+1+diff_len]
                        probl_list.reverse()
                        new_list = probl_list[:actual_cols_no-1]+ ['|'.join(probl_list[actual_cols_no-1:])]
                        new_list.reverse()
                        actual_list = line_list[:prob_cols[0]] + new_list + line_list[prob_cols[1]+1+diff_len:]
                    else:
                        continue
                else:
                    if prob_cols:
                        # not proper
                        # new_list = line_list[:prob_cols[0]] + '|'.join(line_list[prob_cols[0]:prob_cols[1]+1+diff_len])+\
                        #                 line_list[prob_cols[1]+1+diff_len:]
                        diff_len = n_cols - len(line_list)
                        probl_list = line_list[prob_cols[0]:prob_cols[1]+1-diff_len]
                        probl_list.reverse()
                        new_list = probl_list+ ['']*diff_len
                        new_list.reverse()
                        actual_list = line_list[:prob_cols[0]] + new_list + line_list[prob_cols[1]+1:]
                    else:
                        continue
                out_dic[ind] = actual_list
                ind += 1
                if ind == chunk_size:
                    out_df = pd.DataFrame.from_dict(out_dic,'index')
                    out_df.columns = cols
                    yield out_df
                    out_dic = {}
                    ind = 0
                    chunk_ind += 1
            out_df = pd.DataFrame.from_dict(out_dic,'index')
            out_df.columns = cols
            yield out_df

