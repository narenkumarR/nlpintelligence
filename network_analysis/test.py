__author__ = 'joswin'

import re
with open('company_crawled_15April_2.txt','r') as f:
    tmp = f.readlines()

with open('company_crawled_2016-04-22.txt','r') as f:
    tmp1 = f.readlines()

tmp = tmp+tmp1
del tmp1
node_list = []
edge_list = []
from random import shuffle
shuffle(tmp)
f=tmp[:200]
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

pos=nx.fruchterman_reingold_layout(G)

Xv=[pos[k][0] for k in node_list]
Yv=[pos[k][1] for k in node_list]

Xed=[]
Yed=[]
for edge in edge_list:
    Xed+=[pos[edge[0]][0],pos[edge[1]][0], None]
    Yed+=[pos[edge[0]][1],pos[edge[1]][1], None]


import plotly.plotly as py
from plotly.graph_objs import *
trace3=Scatter(x=Xed,
               y=Yed,
               mode='lines',
               line=Line(color='rgb(210,210,210)', width=1),
               hoverinfo='none'
               )
trace4=Scatter(x=Xv,
               y=Yv,
               mode='markers',
               name='net',
               marker=Marker(symbol='dot',
                             size=5,
                             color='#6959CD',
                             line=Line(color='rgb(50,50,50)', width=0.5)
                             ),
               text=node_list,hoverinfo='text')
annot="This networkx.Graph has the Fruchterman-Reingold layout<br>Code:"+\
"<a href='http://nbviewer.ipython.org/gist/empet/07ea33b2e4e0b84193bd'> [2]</a>"

data1=Data([trace3, trace4])
width=800
height=800
axis=dict(showline=False, # hide axis line, grid, ticklabels and  title
          zeroline=False,
          showgrid=False,
          showticklabels=False,
          title=''
          )
layout=Layout(title= "company graph",
    font= Font(size=12),
    showlegend=False,
    autosize=False,
    width=width,
    height=height,
    xaxis=XAxis(axis),
    yaxis=YAxis(axis),
    margin=Margin(
        l=40,
        r=40,
        b=85,
        t=100,
    ),
    hovermode='closest',
    annotations=Annotations([
           Annotation(
           showarrow=False,
            text='This igraph.Graph has the Kamada-Kawai layout',
            xref='paper',
            yref='paper',
            x=0,
            y=-0.1,
            xanchor='left',
            yanchor='bottom',
            font=Font(
            size=14
            )
            )
        ]),
    )
data1=Data([trace3, trace4])
fig1=Figure(data=data1, layout=layout)
fig1['layout']['annotations'][0]['text']=annot
py.iplot(fig1, filename='company-network-nx')