__author__ = 'joswin'
from flask import Flask,request
from flask_restful import Resource, Api
import sys
import json
import requests
import time
import pdb

from optparse import OptionParser

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)
api = Api(app)


headers = {'Accept':'application/json',
           # 'accessToken':'U8nSVwyMAKMn+DTUkewvFNHHQiUpZCoRx+o2c1CQ3ysXXiyOxYK1J+tQf7s3cr4jCmC7zeVvu7Tk9y+3rISSjWtJUWYuUoxBZG0t1gj6BmhTO/cdOSPMRKNwzFru8OoBsyFISpDe3RHJo4JCHzOsnATR7apJbkwUJHuUzBxNRO4=.eyJmZWF0dXJlcyI6Int9IiwiY2xpZW50SWQiOiIxNGJ1NGJhaWoxbWY3cm9lNGJmZyIsImdyYW50VHlwZSI6ImNyZWQiLCJ0dGwiOiIzMTUzNjAwMCIsImlhdCI6IjE0ODQ3MzI3OTYiLCJqdGkiOiIyZWMyNTEzNS0zMTdiLTRiNWUtYWRlNS0zODBhMzdmOWEwNGIifQ=='
           'accessToken':'XDl0o6H4B+FprJ3rG/aloG6fqFwJg+LK4/1Z/uBbAc/s8y1/FbOnLX5wy3pQbRffNcCVMpMOIiJLTY4V1xRQbPHT2b5N44BhHKyv9+8z2pE01mNW0R2ADcc0gFdqcs1ijukze2iSoptp2+yC4GBkjiNsBgQ/Q2NnpE1zL12sRFI=.eyJmZWF0dXJlcyI6Int9IiwiY2xpZW50SWQiOiJndjNhMmI4bGI2dnE3bmhqcDFhYSIsImdyYW50VHlwZSI6ImNyZWQiLCJ0dGwiOiIzMTUzNjAwMCIsImlhdCI6IjE0ODM2OTQxNDkiLCJqdGkiOiIyZGFmNjA1NS00MzI5LTRiNWItYTkyNy1iM2NiZjU1OWJjMWMifQ=='
           }

limiter = Limiter(
    app,
    global_limits=["1000 per 5 minute"],
    key_func=get_remote_address,
)

class RateLimitter(Resource):

    @limiter.limit("1000 per 5 minute",error_message='limit reached')
    def get(self):
        ''' example:
        r = requests.get('http://127.0.0.1:5000/',params={'url':'https://api.insideview.com/api/v1/companies','name':'pipecandy'})
        :return:
        '''
        # print(request.args)
        req_dic = dict(request.args)
        urls = req_dic.pop('url')
        url = urls[0]
        # if not req_dic:
        #     return {'value':'no data provided'}
        # out_dict = get_num_wait(request.args)
        r = requests.get(url,params=req_dic,headers=headers)
        if r.status_code == 429:
            print('request throttled by insideview')
            time.sleep(20)
            return {'message':'request throttled by insideview'}
        try:
            return json.loads(r.text)
        except:
            pass
        # return {'value':1}

    @limiter.limit("1000 per 5 minute",error_message='limit reached')
    def post(self):
        '''
        Give all the parameters as json
        :return:
        '''
        # pdb.set_trace()
        json_data = request.get_json()
        # data = eval(request.data)
        req_dic = dict(request.args)
        urls = req_dic.pop('url')
        url = urls[0]
        r = requests.post(url,params=req_dic,headers=headers,data=json_data)
        if r.status_code == 429:
            print('request throttled by insideview')
            time.sleep(20)
            return {'message':'request throttled by insideview'}
        try:
            return json.loads(r.text)
        except:
            pass
        # return {'value':1}

api.add_resource(RateLimitter, '/')

if __name__ == '__main__':
    optparser = OptionParser()
    optparser.add_option('--ip',
                         dest='ip',
                         help='ip',type='str',
                         default='127.0.0.1')
    optparser.add_option('--port',
                         dest='port',
                         help='port',
                         type='int',
                         default='5000')
    optparser.add_option('--debug',
                         dest='debug',
                         help='set debug option to True if 1',
                         default=1,type='int')
    optparser.add_option('--accesstoken',
                         dest='accesstoken',
                         help='give accesstoken. if not, default dev accesstoken will be taken',
                         default='',type='str')

    (options, args) = optparser.parse_args()
    ip = options.ip
    port = options.port
    debug = options.debug
    accesstoken = options.accesstoken
    if accesstoken:
        headers['accessToken'] = accesstoken

    app.run(host=ip,port=port,debug=True if debug else False,threaded=True)
