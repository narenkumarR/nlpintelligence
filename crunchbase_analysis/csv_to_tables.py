__author__ = 'joswin'

import gc
from sqlalchemy import create_engine

import os,re
path = 'data_2016_07_04/csv_export'
files = os.listdir(path)

from csv_reader import CsvReader
reader = CsvReader()
engine = create_engine('postgresql://postgres:postgres@localhost:5432/linkedin_data')
for f_name in files:
    ind = 0
    print(f_name)
    if re.search('\.csv$',f_name):
        t_name =f_name[:f_name.find('.csv')]
    else:
        continue
    print(t_name)
    ful_name = path+'/'+f_name
    for tmp in reader.read_csv(ful_name,n_cols=False,chunk_size=25000):
        print('no of records: {}, iteration: {}'.format(tmp.shape,ind))
        tmp.to_sql(t_name,engine,index=False,if_exists='append',schema='crunchbase_data')
        del tmp
        gc.collect()
        ind += 1

