from sqlalchemy import create_engine
import pandas as pd
engine = create_engine('postgresql://pipecandy_user:pipecandy@192.168.1.142:5432/pipecandy_db1')
df = pd.read_sql_query("select * from tmp_sample_ecom_rajesh1_people",con=engine)
import name_tools
def name_split(name):
    name1 = name.split(',')[0]
    name1 = re.sub('[!@#$%^&*()}{></~+_]|\[|\]',' ',name1)
    name1 = re.sub(' +',' ',name1)
    name_cleaned = name_tools.split(name1)
    f_part = name_cleaned[1].split()
    if len(f_part) == 1:
        f_name,m_name = f_part[0],''
    elif len(f_part) > 1:
        f_name,m_name = f_part[0],' '.join(f_part[1:])
    else:
        f_name,m_name = '',''
    name_list = [name_cleaned[0],f_name,m_name,name_cleaned[2],name_cleaned[3]]
    return name_list

f_names ,m_names,l_names=[],[],[]
for name in list(df['name']):
    names = name_split(name)
    f_names.append(names[1])
    m_names.append(names[2])
    l_names.append(names[3])

df['first_name']=f_names
df['middle_name']=m_names
df['last_name']=l_names

emails =   []
for ind in df.index:
    f,m,l,domain = df.ix[ind,['first_name','middle_name','last_name','domain']]
    tmp = f+'.'+m+'.'+l+'@'+domain
    tmp = tmp.encode('ascii','ignore')
    tmp = re.sub('^\.','',tmp.encode('ascii','ignore'))
    tmp = re.sub('\.+','.',tmp)
    if tmp[0]=='.':
        emails.append('')
    else:
        emails.append(tmp)

df['email']=emails
df1 = df[df['email']!='']
df1.to_excel('/home/madan/Desktop/joswin_bck/toPendrive/works/nlp-intelligence/misc/data_for_rajesh_ui/ecommerce_sample_data_for_ui_with_people.xls' ,index=False)


df2 = df1[df1['country'].str.contains("[a-z]")==False]
df2.index = range(df2.shape[0])
import random
def get_sample_data(value_list,list_len):
    out = [random.choice(value_list) for _ in range(list_len)]
    return out

no_of_products = ['0-5,000','5,000-50,000','50,000-200,000','200,000-500,000','500,000-1,000,000','>1,000,000','Not Available']
tmp = get_sample_data(no_of_products,df2.shape[0])
df2['no_of_products'] = tmp

no_of_unique_visitors = ['0-50,000','50,000-200,000','200,000-1,000,000','1,000,000-5,000,000','5,000,000-10,000,000','>10,000,000','Not Available']
tmp1 = [no_of_unique_visitors[min(max(no_of_products.index(i) + random.choice([-1,0,1]),0),len(no_of_unique_visitors)-1)] for i in tmp]
df2['no_of_unique_visitors'] = tmp1

channel_presence = ['Online', 'Mobile', 'B&M', 'MultiChannel']
tmp = get_sample_data(channel_presence,df2.shape[0])
df2['channel_presence'] = tmp

market_place_type = ['Merchants', 'Peer-2-Peer', 'Comparison Shopping', 'Info Aggregator']
tmp = get_sample_data(market_place_type,df2.shape[0])
df2['market_place_type'] = tmp

browse_and_filter = [0,1]
tmp = get_sample_data(browse_and_filter,df2.shape[0])
df2['browse_and_filter'] = tmp

faceted_search = [0,1]
tmp = get_sample_data(browse_and_filter,df2.shape[0])
df2['faceted_search'] = tmp

recommendation_coll_filtering = [0,1]
tmp = get_sample_data(browse_and_filter,df2.shape[0])
df2['recommendation_coll_filtering'] = tmp

visual_merchandising = [0,1]
tmp = get_sample_data(browse_and_filter,df2.shape[0])
df2['visual_merchandising'] = tmp
df2.to_excel('/home/madan/Desktop/joswin_bck/toPendrive/works/nlp-intelligence/misc/data_for_rajesh_ui/ecommerce_sample_data_for_ui_with_people1.xls' ,index=False)
