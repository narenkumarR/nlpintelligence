__author__ = 'joswin'
# -*- coding: utf-8 -*-
from sklearn.decomposition import NMF, LatentDirichletAllocation
from sklearn.cluster import KMeans

def print_top_words(model, feature_names, n_top_words):
    for topic_idx, topic in enumerate(model.components_):
        print("Topic #%d:" % topic_idx)
        print(" ".join([feature_names[i]
                        for i in topic.argsort()[:-n_top_words - 1:-1]]))
    print()

def print_top_words_in_km_cluster(km,num_clusters,terms):
    print("Top terms per cluster:")
    print()
    #sort cluster centers by proximity to centroid
    order_centroids = km.cluster_centers_.argsort()[:, ::-1]
    for i in range(num_clusters):
        print("Cluster {i}: {words}".format(i=i,
              words = ' '.join([terms[ind] for ind in order_centroids[i, :10]])
              )
        )
    print()

class Unsupervised(object):
    '''
    '''
    def __init__(self):
        '''
        :return:
        '''
        pass

    def generate_topics(self,data_matrix,feature_names,algorithm,n_topics,n_top_words,**kwargs):
        '''
        :param data_matrix: data_matrix (usually document term matrix
        :param feature_names: the names of each column
        :param algorithm: the algorithm to run ('lda','nmf')
        :param n_topics:
        :param n_top_words:
        :param kwargs:
        :return:
        '''
        if algorithm == 'lda':
            clf = LatentDirichletAllocation(n_topics=n_topics, max_iter=5,
                                learning_method='online', learning_offset=50.,
                                random_state=0,**kwargs)
        elif algorithm == 'nmf':
            clf = NMF(n_components=n_topics, random_state=1, alpha=.1, l1_ratio=.5)
        data_transformed = clf.fit_transform(data_matrix)
        print_top_words(clf, feature_names, n_top_words)
        return clf, data_transformed

    def generate_clusters(self,data_matrix,feature_names,num_clusters,n_top_words,algorithm='kmeans',**kwargs):
        '''
        :param data_matrix: data_matrix (usually document term matrix
        :param feature_names: the names of each column
        :param algorithm: the algorithm to run
        :param num_clusters:
        :param n_top_words:
        :param kwargs:
        :return:
        '''
        if algorithm == 'kmeans':
            km = KMeans(n_clusters=num_clusters)
            km.fit(data_matrix)
            clusters = km.labels_.tolist()
            clust_dists = km.transform(data_matrix)
            print_top_words_in_km_cluster(km,num_clusters,feature_names)
            return km,clusters,clust_dists