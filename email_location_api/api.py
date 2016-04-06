__author__ = 'joswin'
from flask import Flask,request
from flask_restful import Resource, Api
import sys

from email_location.email_location_finder import EmailLocationFinder

loc_finder = EmailLocationFinder()

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
        json_data = request.get_json()
        out_dict = {}
        for key in json_data:
            out_dict[key] = loc_finder.get_location_ddg_linkedin(json_data[key])
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
    else:
        app.run(debug=True)