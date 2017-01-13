__author__ = 'akshaya'
from __future__ import print_function


from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.pipeline import make_pipeline
from sklearn.naive_bayes import MultinomialNB
from sklearn.preprocessing import Normalizer
from sklearn import metrics
from sklearn.cluster import KMeans, MiniBatchKMeans
import logging
from optparse import OptionParser

import csv
from time import time

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# Display progress logs on stdout
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# parse commandline arguments
op = OptionParser()
op.add_option("--lsa",
              dest="n_components", type="int",
              help="Preprocess documents with latent semantic analysis.")
op.add_option("--no-minibatch",
              action="store_false", dest="minibatch", default=True,
              help="Use ordinary k-means algorithm (in batch mode).")
op.add_option("--no-idf",
              action="store_false", dest="use_idf", default=True,
              help="Disable Inverse Document Frequency feature weighting.")
op.add_option("--use-hashing",
              action="store_true", default=False,
              help="Use a hashing feature vectorizer")
op.add_option("--n-features", type=int, default=10000,
              help="Maximum number of features (dimensions)"
                   " to extract from text.")
op.add_option("--verbose",
              action="store_true", dest="verbose", default=False,
              help="Print progress reports inside k-means algorithm.")
op.add_option('-i', '--inputFile',
                     dest='inputFile',
                     help='To read data from Input File instead of DB',
                     default=None)
op.add_option('-F', '--predictFile',
                     dest='predictFile',
                     help='To read data from Input File instead of DB',
                     default=None)

print(__doc__)
op.print_help()

(opts, args) = op.parse_args()
'''
if len(args) > 0:
    op.error("this script takes no arguments.")
    sys.exit(1)
'''

inputFile = opts.inputFile
###############################################################################
# Load some categories from the training set
'''categories = [
    'alt.atheism',
    'talk.religion.misc',
    'comp.graphics',
    'sci.space',
]
'''
dataset = []
cont=0
with open(inputFile,'rbU') as fin:
      rows = csv.reader(fin)
     # rows.next()
      for row in rows:
          if row and row[2]:
               cont=cont+1
               dataset.append(row[2])
fin.close()
print("THIS IS LEARNING DATASET with values")
print(cont)
print(dataset)
predictFile = opts.predictFile
predictset = []
count = 0
with open(predictFile,'rbU') as fin:
      rows = csv.reader(fin)
     # rows.next()
      for row in rows:
          if row and row[2]:
               count =count + 1
               predictset.append(row[2])
fin.close()
print("THIS IS PREDICTING DATASET with values")
print(count)
print(predictset)
# Uncomment the following to do the analysis on all the categories
categories = None

print("Loading 20 newsgroups dataset for categories:")
print(categories)

learn_set = []
with open("F:\PipeCandy\DATA\input1\learnset.csv",'rbU') as fin:
      count = 0
      rows = csv.reader(fin)
     # rows.next()
      for row in rows:
          if row and row[0]:
               count =count + 1
               learn_set.append(row[0])
fin.close()
print("THIS IS LEARNING VALUES with value")
print(count)
print(learn_set)

#dataset = fetch_20newsgroups(subset='all', categories=categories,
              #               shuffle=True, random_state=42)


#print("%d documents" % len(dataset.data))
#print("%d categories" % len(dataset.target_names))
print()

labels = dataset
true_k = 8

print("Extracting features from the training dataset using a sparse vectorizer")
t0 = time()
if opts.use_hashing:
    if opts.use_idf:
        # Perform an IDF normalization on the output of HashingVectorizer
        hasher = HashingVectorizer(n_features=opts.n_features,
                                   stop_words='english', non_negative=True,
                                   norm=None, binary=False)
        vectorizer = make_pipeline(hasher, TfidfTransformer())
    else:
        vectorizer = HashingVectorizer(n_features=opts.n_features,
                                       stop_words='english',
                                       non_negative=False, norm='l2',
                                       binary=False)
else:
    vectorizer = TfidfVectorizer(max_df=0.5, max_features=opts.n_features,
                                 min_df=2, stop_words='english',
                                 use_idf=opts.use_idf)
X = vectorizer.fit_transform(dataset)
Y = vectorizer.transform(predictset)
print(X)
print("done in %fs" % (time() - t0))
print("n_samples: %d, n_features: %d" % X.shape)
print()

if opts.n_components:
    print("Performing dimensionality reduction using LSA")
    t0 = time()
    # Vectorizer results are normalized, which makes KMeans behave as
    # spherical k-means for better results. Since LSA/SVD results are
    # not normalized, we have to redo the normalization.
    svd = TruncatedSVD(opts.n_components)
    normalizer = Normalizer(copy=False)
    lsa = make_pipeline(svd, normalizer)

    X = lsa.fit_transform(X)
    Y = lsa.transform(Y)

    print("done in %fs" % (time() - t0))

    explained_variance = svd.explained_variance_ratio_.sum()
    print("Explained variance of the SVD step: {}%".format(
        int(explained_variance * 100)))

    print()
clf = MultinomialNB()
#datarray=np.array(dataset).reshape(-1, 1)
#yarray=np.array(Y).reshape(-1,1)
#Y.reshape(-1,1)
clf.fit(X, learn_set)
result=clf.predict(Y)
print("THIS IS RESULTTT")
print(result)