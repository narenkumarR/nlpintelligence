__author__ = 'joswin'

import pdb
import re
import sys
from flask import Flask,request
from flask_restful import Resource, Api

from constants import people_detail_table,people_detail_table_cols,people_detail_table_cols_str
from postgres_connect import PostgresConnect
from gen_people_for_email import gen_people_details

app = Flask(__name__)
api = Api(app)
con = PostgresConnect()

class PeopleForEmail(Resource):
    def get(self):
        '''
        :return:
        '''
        cut_off = request.args.get('cutoff')
        list_name = request.args.get('listname')
        if not list_name or list_name == 'NULL':
            return {'Error':'No listname provided'}
        con.get_cursor()
        query = "select id from {} where list_name = %s".format('crawler.list_table')
        con.cursor.execute(query,(list_name,))
        tmp = con.cursor.fetchall()
        if not tmp:
            return {'Error':'List name provided is not present in the tables'}
        list_id = tmp[0][0]
        gen_people_details(list_id)
        if not cut_off or cut_off == 'NULL':
            query = 'select {} from {} where list_id = %s order by created_on desc'.format(people_detail_table_cols_str,people_detail_table)
            con.get_cursor()
            con.cursor.execute(query,(list_id,))
            res_list = con.cursor.fetchall()
            con.close_cursor()
        else:
            re_match = re.search(r'^\d{4}-\d{2}-\d{2}',cut_off)
            if re_match:
                cut_off = re.sub('_',' ',cut_off)
                query = 'select {} from {} where created_on >= %s and list_id = %s order by created_on desc'.format(people_detail_table_cols_str,people_detail_table)
                con.get_cursor()
                con.cursor.execute(query,(cut_off,list_id,))
                res_list = con.cursor.fetchall()
                con.close_cursor()
            else:
                return {'Error':'cutoff is not in proper format'}
        out_dic = {}
        for res in res_list:
            tmpdic = {}
            for ind in range(len(res)):
                tmpdic[people_detail_table_cols[ind]] = str(res[ind])
            out_dic[tmpdic['id']] = tmpdic
        return out_dic

    def post(self,time_cutoff = None):
        '''
        Need to give json with key:text format
        :return:
        '''
        # json input data should be of the form {key1:{json1},key2:{json2}..}
        # json1,json2 etc can have two forms: {'Email':email_id} or {'Name':name,'Company':company}
        # in addition json1,json2 have key 'Method', with possible values 'Fast' or 'Best'. If no key 'best' is assumed
        return {'Error':'only get allowed'}

    def put(self):
        '''
        Need to give json with key:text,key_class:class format
        :return:
        '''
        return {'Error':'only get allowed'}


api.add_resource(PeopleForEmail, '/')

if __name__ == '__main__':
    port = 80
    if len(sys.argv)==3:
        ip = sys.argv[1]
        # port = sys.argv[2]
        app.run(host=ip,port=port)
    elif len(sys.argv) == 2:
        ip = sys.argv[1]
        app.run(host=ip,port=port)
    else:
        app.run(port = port,debug=True,threaded=True)
