import scrapy
from scrapy.http import Request

class DmozSpider(scrapy.Spider):
    name = "dmoz"
    allowed_domains = ["dmoz.org"]
    start_urls = [
        "http://www.dmoz.org/Computers/Programming/Languages/Python/Books/",
        "http://www.dmoz.org/Computers/Programming/Languages/Python/Resources/"
    ]

    def parse(self, response):
        filename = response.url.split("/")[-2]
        with open(filename, 'wb') as f:
            f.write(response.body)


class IpSpider(scrapy.Spider):
    name = "ip_test"
    allowed_domains = ["lumtest.com"]
    start_urls = [
        "https://lumtest.com/myip.json"
    ]

    def parse(self, response):
        filename = response.url.split("/")[-1]
        with open(filename, 'wb') as f:
            f.write(response.body)

class IpSpider_proxy(scrapy.Spider):
    name = "ip_test_proxy"
    allowed_domains = ["lumtest.com","linkedin.com"]
    start_urls = [
        "https://lumtest.com/myip.json",
        "https://www.linkedin.com/in/mikael-friis-9442ab"
    ]
    handle_httpstatus_list = [999]
    def __init__(self, *args, **kwargs):
        self.proxy_pool = ['https://lum-customer-pipecan-zone-gen:d6921a3c6904@zproxy.luminati.io:22225']
        scrapy.Spider.__init__(self,*args,**kwargs)


    def parse(self, response):
        filename = response.url.split("/")[-1]
        with open('proxy'+filename, 'a') as f:
            f.write(response.body)
            f.write('\n')

    def make_requests_from_url(self, url):
        req = Request(url=url)
        if self.proxy_pool:
            proxy_addr = self.proxy_pool[0]
            req.meta['proxy'] = proxy_addr
        return req