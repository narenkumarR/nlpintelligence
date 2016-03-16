__author__ = 'joswin'
#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd,re
from nltk.corpus import stopwords
import pdb

import word_transformations as PPP
import clean_mails

tk = PPP.Tokenizer()

stop_words=stopwords.words('english')
stop_words = list(set(stop_words) - set(['not','no']))

