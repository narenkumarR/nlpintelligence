__author__ = 'joswin'
from url_cleaner import UrlCleaner

class SoupUtils(object):

    def __init__(self):
        self.url_cleaner = UrlCleaner()

    def get_text_from_soup(self,soup):
        '''
        :param soup:
        :return:
        '''
        for script in soup(["script", "style"]):
            script.extract()    # rip it out
        # get text
        text = soup.get_text(' ')
        # break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        # break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)
        return text

    def get_all_links_soupinput(self,soup,base_url):
        '''
        :param soup:
        :return:
        '''
        url_list, mail_list = [],[]
        url_tmp = []
        for a in soup.find_all('a', href=True):
            url = a['href']
            if '@' in url:
                mail_list.append(url)
            else:
                url = self.url_cleaner.merge_urls(base_url,url)
                url = self.url_cleaner.clean_url(url)
                if url not in url_tmp:
                    url_list.append((url,a.text))
                    url_tmp.append(url)
        return url_list,mail_list
