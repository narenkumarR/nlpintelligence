__author__ = 'joswin'
import re
from url_cleaner import UrlCleaner

tags = ['h1','h2','h3','h4','h5','h6','p']
class SoupUtils(object):

    def __init__(self):
        self.url_cleaner = UrlCleaner()

    def get_text_from_soup(self,soup):
        '''
        :param soup:
        :return:
        '''
        # for script in soup(["script", "style"]):
        #     script.extract()    # rip it out
        # get text
        text = soup.get_text(' ')
        # break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        # break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)
        return text

    def get_text_cleaned_format_from_soup(self,soup):
        '''
        :param soup:
        :return:
        '''
        meta_text = ' '.join([i.get('content','')
                 for i in soup.find_all('meta')
                    if re.search('description|keyword',i.get('name','none'),re.IGNORECASE) or
                     re.search('description|keyword',i.get('property','none'),re.IGNORECASE) ])
        tag_texts_list = []
        for tag in tags:
            tag_texts_list.extend([i.text for i in soup.find_all(tag)])
        tag_texts = '. '.join(tag_texts_list)
        url_text = '. '.join([i.text.strip() for i in soup.find_all('a') if i.text.strip()])
        return meta_text,tag_texts,url_text

    def get_all_links_soupinput(self,soup,base_url,merge_urls=True,only_visible=True):
        '''
        :param soup:
        :param base_url:
        :param merge_urls:
        :return:
        '''
        # if only_visible:
        #     [s.extract() for s in soup(['style', 'script', '[document]', 'head', 'title'])]
        url_list, mail_list = [],[]
        url_tmp = []
        for a in soup.find_all('a', href=True):
            url = a['href']
            if url == base_url:
                continue
            if '@' in url:
                mail_list.append(url)
            else:
                if merge_urls:
                    url = self.url_cleaner.merge_urls(base_url,url)
                url = self.url_cleaner.clean_url(url)
                if url not in url_tmp:
                    try:
                        url_list.append((url,a.text.strip()))
                        url_tmp.append(url)
                    except:
                        continue
        return url_list,mail_list
