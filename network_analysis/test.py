__author__ = 'joswin'

import re

node_list = []
edge_list = []
with open('company_crawled_2016-04-22.txt','r') as f:
    for line in f:
        tmp = eval(line)
        main_url = tmp['Linkedin URL']
        node_list.append(main_url)
        if 'Also Viewed Companies' in tmp:
            for i in tmp['Also Viewed Companies']:
                link = i['company_linkedin_url']
                link = re.sub(r'\?trk=pub-pbmap|\?trk=prof-samename-picture|\?trk=extra_biz_viewers_viewed','',link)
                node_list.append(link)
                if main_url == link:
                    continue
                elif main_url < link:
                    edge_list.append((main_url,link))
                else:
                    edge_list.append((link,main_url))

node_list = list(set(node_list))
edge_list = list(set(edge_list))

import networkx as nx
G=nx.Graph()
G.add_nodes_from(node_list)
G.add_edges_from(edge_list)
