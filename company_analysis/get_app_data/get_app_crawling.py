__author__ = 'joswin'

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

class GetAppCompanyPage():
    def __init__(self):
        pass

    def get_pricing(self):
        try:
            dets = self.soup.find('div',{'id':'pricing'}).findNext('div',{'class':'col-lg-5'}).find_all('div')
            if not dets:
                dets = self.soup.find('div',{'id':'pricing'}).findNext('div',{'class':'col-lg-5'}).find_all('p')
            for det in dets:
                try:
                    self.res_dic[det.find('strong').text[:-1]] = det.text
                except:
                    pass
        except:
            pass

    def get_key_features(self):
        try:
            det = [i.text for i in self.soup.find('div',{'id':'features'}).findNext('div',{'class':'row'}).find_all('li')]
            self.res_dic['key_features'] = det
        except:
            pass

    def get_description(self):
        try:
            self.res_dic['description'] = self.soup.find('div',{'id':'overview'}).findNext('div',{'itemprop':'description'}).text
        except:
            pass

    def get_website(self):
        try:
            website = self.soup.find('div',{'id':'overview'}).findNext('div',{'itemprop':'description'}).\
                findNext('div',{'class':'row'}).find('span',{'class':'text-muted'}).text.strip()
        except:
            try:
                website = self.soup.find('div',{'id':'overview'}).findNext('div',{'itemprop':'description'}).find('a').text.strip()
            except:
                website = ''
        self.res_dic['Website'] = website

    def get_specifications(self):
        ''' could be optimized
        :param soup2:
        :param res_dic:
        :return:
        '''
        try:
            self.res_dic[self.soup.find('div',{'id':'specifications'}).\
                findNext('div',{'class':'row'}).\
                find('strong').text] = self.soup.find('div',{'id':'specifications'}).\
                                            findNext('div',{'class':'row'}).\
                                            find('strong').findNext('div').text
            self.res_dic[self.soup.find('div',{'id':'specifications'}).\
                findNext('div',{'class':'row'}).\
                findNext('div',{'class':'row'}).\
                find('strong').text] = self.soup.find('div',{'id':'specifications'}).\
                                            findNext('div',{'class':'row'}).\
                                            findNext('div',{'class':'row'}).\
                                            find('strong').findNext('div').text
            self.res_dic[self.soup.find('div',{'id':'specifications'}).\
                findNext('div',{'class':'row'}).\
                findNext('div',{'class':'row'}).\
                findNext('div',{'class':'row'}).\
                find('strong').text] = self.soup.find('div',{'id':'specifications'}).\
                                            findNext('div',{'class':'row'}).\
                                            findNext('div',{'class':'row'}).\
                                            findNext('div',{'class':'row'}).\
                                            find('strong').findNext('div').text
            self.res_dic[self.soup.find('div',{'id':'specifications'}).\
                findNext('div',{'class':'row'}).\
                findNext('div',{'class':'row'}).\
                findNext('div',{'class':'row'}).\
                findNext('div',{'class':'row'}).\
                find('strong').text] = self.soup.find('div',{'id':'specifications'}).\
                                            findNext('div',{'class':'row'}).\
                                            findNext('div',{'class':'row'}).\
                                            findNext('div',{'class':'row'}).\
                                            findNext('div',{'class':'row'}).\
                                            find('strong').findNext('div').text
            self.res_dic[self.soup.find('div',{'id':'specifications'}).\
                findNext('div',{'class':'row'}).\
                findNext('div',{'class':'row'}).\
                findNext('div',{'class':'row'}).\
                findNext('div',{'class':'row'}).\
                findNext('div',{'class':'row'}).\
                find('strong').text] = self.soup.find('div',{'id':'specifications'}).\
                                            findNext('div',{'class':'row'}).\
                                            findNext('div',{'class':'row'}).\
                                            findNext('div',{'class':'row'}).\
                                            findNext('div',{'class':'row'}).\
                                            findNext('div',{'class':'row'}).\
                                            find('strong').findNext('div').text

            categories = self.soup.find('div',{'id':'specifications'}).\
                                    findNext('div',{'class':'row'}).\
                                    findNext('div',{'class':'row'}).\
                                    findNext('div',{'class':'row'}).\
                                    findNext('div',{'class':'row'}).\
                                    findNext('div',{'class':'row'}).\
                                    findNext('div',{'class':'row'}).\
                                    find('strong').findNext('div').find_all('a')
            categories = [i.text for i in categories]
            categories = [i for i in chunks(categories,2)]
            self.res_dic['categories'] = categories
        except:
            pass

    def get_benefits(self):
        try:
            self.res_dic['benefits'] = self.soup.find('div',{'id':'benefits'}).findNext('div').text
        except:
            pass

    def get_whois_for(self):
        try:
            lis = self.soup.find('div',{'class':'body-container'}).find('ul').find_all('li')
            for li in lis:
                try:
                    self.res_dic[li.text.split(':')[0][:-1]]=li.text.split(':')[1]
                except:
                    pass
        except:
            pass

    def get_details(self,soup):
        ''' 
        :param soup: 
        :return:
        '''
        self.soup = soup
        self.res_dic = {}
        self.get_pricing()
        self.get_key_features()
        self.get_description()
        self.get_website()
        self.get_specifications()
        self.get_benefits()
        return self.res_dic


