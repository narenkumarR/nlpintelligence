__author__ = 'joswin'
import re

common_company_wrds = ['limited', 'all', 'just', 'being', 'over', 'both', 'through', 'solutions', 'pty', 'yourselves',
                       'llc', 'its', 'before', 'partners', 'group', 'with', 'had', 'should', 'to', 'only', 'systems',
                       'under', 'ours', 'has', 'do', 'them', 'his', 'very', 'de', 'they', 'new', 'not', 'during',
                       'now', 'him', 'nor', 'school', 'did', 'these', 'each', 'where', 'because', 'pvt', 'doing',
                       'theirs', 'some', 'global', 'gmbh', 'design', 'are', 'our', 'ourselves', 'out', 'what',
                       'network', 'for', 'capital', 'below', 'creative', 'does', 'health', 'above', 'between',
                       'international', 'she', 'be', 'we', 'after', 'business', 'web', 'marketing', 'here', 'corp',
                       'hers', 'by', 'on', 'about', 'of', 'against', 'ltd', 'com', 'or', 'software', 'consulting',
                       'own', 'co', 'into', 'association', 'private', 'yourself', 'down', 'your', 'management', 'from',
                       'her', 'whom', 'there', 'been', 'few', 'too', 'communications', 'themselves', 'was', 'until',
                       'more', 'himself', 'that', 'company', 'but', 'off', 'technologies', 'herself', 'than', 'those',
                       'he', 'me', 'myself', 'this', 'up', 'will', 'while', 'associates', 'can', 'were', 'my', 'and',
                       'then', 'is', 'in', 'am', 'it', 'an', 'as', 'itself', 'at', 'have', 'further', 'technology',
                       'their', 'if', 'again', 'no', 'media', 'agency', 'when', 'same', 'any', 'how', 'other', 'which',
                       'digital',  'inc', 'development', 'who', 'most', 'services', 'such', 'why', 'engineering',
                        'center', 'medical', 'having', 'so', 'corporation', 'the', 'yours', 'once']

company_common_reg = re.compile(r'\b'+r'\b|\b'.join(common_company_wrds)+r'\b')
