__author__ = 'joswin'
from flask import Flask,request
from flask_restful import Resource, Api
import sys
import pdb

from company_linkedin_url_extractor.company_extractor import CompanyLinkedinURLExtractorMulti
cc = CompanyLinkedinURLExtractorMulti()
app = Flask(__name__)
api = Api(app)

class LinkedinExtractor(Resource):
    def get(self):
        return {'status':'Working'}

    def post(self):
        '''
        Need to give json with key:url format
        :return:
        '''
        json_data = request.get_json()
        if 'timeout' in json_data:
            timeout = int(json_data['timeout'])
            json_data.pop('timeout',None)
        else:
            timeout = 30
        if 'n_threads' in json_data:
            n_threads = int(json_data['n_threads'])
            json_data.pop('n_threads',None)
        else:
            n_threads = 5
        # pdb.set_trace()
        out_dict = cc.get_linkedin_url_multi(json_data,time_out=timeout,n_threads=n_threads)
        return out_dict

api.add_resource(LinkedinExtractor, '/')

if __name__ == '__main__':
    # pdb.set_trace()
    if len(sys.argv)==3:
        ip = sys.argv[1]
        port = sys.argv[2]
        app.run(host=ip,port=int(port),debug=True)
    else:
        app.run(debug=True)