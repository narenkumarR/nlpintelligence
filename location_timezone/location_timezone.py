#!/usr/bin/env python
import pickle
from geopy import geocoders

class TimezoneExtractor(object):
    ''' class to extract timezone from different type of inputs
    '''
    def __init__(self,data_dic_loc = 'tz_data_dic.pkl',
                 key_order = ['15000 population','5000 population','1000 population','web data']):
        with open(data_dic_loc,'r') as f:
            self.tz_data_dic = pickle.load(f)
        self.key_order = key_order
        self.geocoder_web = geocoders.GoogleV3()

    def save_dic(self,loc='tz_data_dic.pkl'):
        '''
        :param loc:
        :return:
        '''
        with open(loc,'w') as f:
            pickle.dump(self.tz_data_dic,f)

    def load_dic(self,loc='tz_data_dic.pkl'):
        '''
        :param loc:
        :return:
        '''
        with open(loc,'r') as f:
            self.tz_data_dic = pickle.load(f)

    def get_best_match(self,tz_out,location_fulltext):
        '''
        :param tz_out:
        :param location_fulltext:
        :return:
        '''
        location_words = set([i.lower() for i in location_fulltext.split(' ')])
        tz_out_wrds = [[i.lower() for i in out.split('/')] for out in list(tz_out)]
        tz_out_counts = []
        for out_wrds in tz_out_wrds:
            match_len = len(set(out_wrds) & location_words)
            tz_out_counts.append(match_len)
        if tz_out_counts.count(max(tz_out_counts)) == 1:
            max_ind = tz_out_counts.index(max(tz_out_counts))
            return list(tz_out)[max_ind]
        else:
            return ''

    def find_tz(self,location,location_fulltext=''):
        '''
        :param location: should be location name cleaned
        :param location_fulltext : full text of the location. If more than 1 result come, will try to do string matching
        to give the result with most match
        :return:
        '''
        location_l = location.lower()
        # first try to match in the tz_data dict

        for key in self.key_order:
            if location_fulltext:
                if (location_l,location_fulltext.lower()) in self.tz_data_dic[key]:
                    tz_out = self.tz_data_dic[key][(location_l,location_fulltext.lower())]
                    if len(tz_out) == 1:
                        return list(tz_out)[0]
            else:
                if location_l in self.tz_data_dic[key]:
                    tz_out = self.tz_data_dic[key][location_l]
                    if len(tz_out) == 1:
                        return list(tz_out)[0]
                    else:
                        if location_fulltext:
                            tmp = self.get_best_match(tz_out,location_fulltext)
                            if tmp:
                                self.tz_data_dic[key][(location_l,location_fulltext.lower())] = set(tmp)
                                return tmp
        # if no match till here, get the location from web
        return self.find_tz_web(location,location_fulltext)

    def find_tz_web(self,location,location_fulltext):
        '''
        :param location:
        :param location_fulltext:
        :return:
        '''
        if location_fulltext:
            try:
                tz_loc = str(self.geocoder_web.timezone(self.geocoder_web.geocode(location+' '+location_fulltext).point))
                self.tz_data_dic['web data'][(location.lower(),location_fulltext.lower())] = set([tz_loc])
                return tz_loc
            except:
                pass
        try:
            tz_loc = str(self.geocoder_web.timezone(self.geocoder_web.geocode(location).point))
            self.tz_data_dic['web data'][location.lower()] = set([tz_loc])
            self.tz_data_dic['web data'][(location.lower(),location_fulltext.lower())] = set([tz_loc])
            return tz_loc
        except:
            pass
        return ''

