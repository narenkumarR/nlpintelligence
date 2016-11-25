# Importing base64 library because we'll need it ONLY in case if the proxy we are going to use requires authentication
import base64
 
# Start your middleware class
class ProxyMiddleware(object):
    # overwrite process request
    def process_request(self, request, spider):
        # Set the location of the proxy
        # request.meta['proxy'] = "https://lum-customer-pipecan-zone-gen:d6921a3c6904@zproxy.luminati.io:22225"
        request.meta['proxy'] = "https://zproxy.luminati.io:22225"
        proxy_user_pass = "lum-customer-pipecan-zone-gen:d6921a3c6904"
        encoded_user_pass = base64.encodestring(proxy_user_pass)
        request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass