__author__ = 'joswin'
import re
import name_tools
def name_cleaner(name):
    '''
    :param name:
    :return:[f_name,m_name,l_name]
    '''
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
    name_list = [f_name,m_name,name_cleaned[2]]
    return name_list

#####testing
###for rails companies
import pandas as pd
rails1 = pd.read_csv('/home/madan/Desktop/joswin_bck/toPendrive/works/nlp-intelligence/Concierge/rails_companies/rails_startups_people_crunchbase.csv')
rails2 = pd.read_csv('/home/madan/Desktop/joswin_bck/toPendrive/works/nlp-intelligence/Concierge/rails_companies/rails_startups_people_linkedin.csv')

f_names,m_names,l_names = [],[],[]
for name in rails2['name']:
    f_name,m_name,l_name = name_cleaner(name)
    f_names.append(f_name)
    m_names.append(m_name)
    l_names.append(l_name)


#####generating queries for table

