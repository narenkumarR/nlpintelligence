__author__ = 'joswin'
from flask import Flask,request
from flask_restful import Resource, Api
import sys
import pdb
# from email_location.email_location_finder import EmailLocationFinder

# loc_finder = EmailLocationFinder()
from email_location.parallel_wrapper1 import Worker
worker = Worker()

app = Flask(__name__)
api = Api(app)

class NBModel(Resource):
    def get(self):
        return {'status':'app running'}

    def post(self):
        '''
        Need to give json with key:text format
        :return:
        '''
        # json input data should be of the form {key1:{json1},key2:{json2}..}
        # json1,json2 etc can have two forms: {'Email':email_id} or {'Name':name,'Company':company}
        # in addition json1,json2 have key 'Method', with possible values 'Fast' or 'Best'. If no key 'best' is assumed
        json_data = request.get_json()
        out_dict = {}
        for key in json_data:
            arg_dict = json_data[key]
            method = arg_dict.get('Method','Best')
            if method == 'Fast':
                out_dict[key] = worker.find_location_fast(arg_dict)
            else:
                out_dict[key] = worker.find_location_best(arg_dict)
        return out_dict

    def put(self):
        '''
        Need to give json with key:text,key_class:class format
        :return:
        '''
        return {'status':'app running'}

api.add_resource(NBModel, '/')

if __name__ == '__main__':
    if len(sys.argv)==3:
        ip = sys.argv[1]
        port = sys.argv[2]
        app.run(host=ip,port=port)
    elif len(sys.argv) == 2:
        ip = sys.argv[1]
        app.run(host=ip)
    else:
        app.run(debug=True,threaded=True)