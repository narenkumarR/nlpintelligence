__author__ = 'joswin'

import os
import time

while True:
    print('starting execution')
    try:
        os.system('sh run_crawl.sh 4000 4')
    except:
        time.sleep(10)
    time.sleep(5)
