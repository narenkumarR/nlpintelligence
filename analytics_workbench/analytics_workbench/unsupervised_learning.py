__author__ = 'joswin'
# -*- coding: utf-8 -*-
from pandas import Series
from sklearn.decomposition import NMF, LatentDirichletAllocation
from sklearn.cluster import KMeans

def print_top_words(model, feature_names, n_top_words):
    for topic_idx, topic in enumerate(model.components_):
        print("Topic #%d:" % topic_idx)
        print(", ".join(['{0}*{1:.2f}'.format(feature_names[i],topic[i])
                        for i in topic.argsort()[:-n_top_words - 1:-1]]))
    print()

def print_top_words_in_km_cluster(km,num_clusters,terms,n_top_words=10):
    print("Top terms per cluster:")
    print()
    #sort cluster centers by proximity to centroid
    order_centroids = km.cluster_centers_.argsort()[:, ::-1]
    for i in range(num_clusters):
        print("Cluster {i}: {words}".format(i=i,
              words = ', '.join([terms[ind] for ind in order_centroids[i, :n_top_words]])
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

    def lda_partial_fit(self,data_matrix_iter,feature_names,n_topics,n_top_words=10,total_samples=1e8,**kwargs):
        ''' when huge data, this method can be used to iterate over the data in chunks
        :param data_matrix_iter: this should be an iterator which gives the data
        :param feature_names:
        :param n_topics:
        :param n_top_words:
        :param kwargs:
        :return:
        '''
        self.clf = LatentDirichletAllocation(n_topics=n_topics, max_iter=5,total_samples=total_samples,
                                learning_method='online', learning_offset=50.,random_state=0,**kwargs)
        for data_matrix in data_matrix_iter:
            self.clf.partial_fit(data_matrix)
        print_top_words(self.clf, feature_names, n_top_words)
        return self.clf

    def generate_topics(self,data_matrix,feature_names,algorithm,n_topics,n_top_words=10,**kwargs):
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
            self.clf = LatentDirichletAllocation(n_topics=n_topics, max_iter=5,
                                learning_method='online', learning_offset=50.,
                                random_state=0,**kwargs)
        elif algorithm == 'nmf':
            self.clf = NMF(n_components=n_topics, random_state=1, alpha=.1, l1_ratio=.5)
        data_transformed = self.clf.fit_transform(data_matrix)
        print_top_words(self.clf, feature_names, n_top_words)
        return self.clf, data_transformed

    def generate_clusters(self,data_matrix,feature_names,num_clusters,n_top_words=10,algorithm='kmeans',**kwargs):
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
            self.clf = KMeans(n_clusters=num_clusters,**kwargs)
            self.clf.fit(data_matrix)
            clusters = self.clf.labels_.tolist()
            clust_dists = self.clf.transform(data_matrix)
            print('No of items in each cluster')
            print(Series(clusters).value_counts())
            print_top_words_in_km_cluster(self.clf,num_clusters,feature_names,n_top_words=n_top_words)
            return self.clf,clusters,clust_dists

    def transform(self,data_matrix,**kwargs):
        '''
        :param data_matrix:
        :param kwargs:
        :return:
        '''
        return self.clf.transform(data_matrix,**kwargs)