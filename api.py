__author__ = 'joswin'
from flask import Flask,request
from flask_restful import Resource, Api
import pdb
import sys

from intent_class_models.naive_bayes.naive_bayes_model import NaiveBayesModel
nb = NaiveBayesModel()
nb.load_model()

app = Flask(__name__)
api = Api(app)

class NBModel(Resource):
    def get(self):
        return {'status':'Model loaded'}

    def post(self):
        '''
        Need to give json with key:text format
        :return:
        '''
        json_data = request.get_json()
        out_dict = {}
        for key in json_data:
            out_dict[key] = nb.predict_textinput(json_data[key])[0]
        return out_dict

    def put(self):
        '''
        Need to give json with key:text,key_class:class format
        :return:
        '''
        json_data = request.get_json()
        dic_len = len(json_data)
        if dic_len<2:
            return 'Not enough data'
        dic_keys = json_data.keys()
        for ind in range(len(json_data)/2):
            text = json_data[dic_keys[2*ind]]
            text_class = json_data[dic_keys[2*ind+1]]
            if text_class in nb.model_classes:
                nb.update_model_single(text,text_class)
        return 'Update completed'

api.add_resource(NBModel, '/')

if __name__ == '__main__':
    if len(sys.argv)==3:
        ip = sys.argv[1]
        port = sys.argv[2]
        app.run(host=ip,port=port)
    else:
        app.run()