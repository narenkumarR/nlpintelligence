from bs4 import BeautifulSoup
import numpy as np
import re,nltk,string,pickle,pdb
from nltk.corpus import stopwords
from gensim.models import word2vec
from sklearn.cluster import KMeans

def doc_to_wordlist( doc, remove_stopwords=False ):
    # Function to convert a document to a sequence of words,
    # optionally removing stop words.  Returns a list of words.
    #
    # 1. Remove HTML
    doc_text = BeautifulSoup(doc).get_text()
    #
    # 2. Remove non-letters
    doc_text = re.sub("[^a-zA-Z]"," ", doc_text)
    #
    # 3. Convert words to lower case and split them
    words = doc_text.lower().split()
    #
    # 4. Optionally remove stop words (false by default)
    if remove_stopwords:
        stops = set(stopwords.words("english"))
        words = [w for w in words if not w in stops]
    #
    # 5. Return a list of words
    return(words)
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
def doc_to_sentences( doc, tokenizer, remove_stopwords=False ):
    # Function to split a para into parsed sentences. Returns a
    # list of sentences, where each sentence is a list of words
    #
    # 1. Use the NLTK tokenizer to split the paragraph into sentences
    raw_sentences = tokenizer.tokenize(doc.strip())
    #
    # 2. Loop over each sentence
    sentences = []
    for raw_sentence in raw_sentences:
        # If a sentence is empty, skip it
        if len(raw_sentence) > 0:
            # Otherwise, call review_to_wordlist to get a list of words
            sentences.append( doc_to_wordlist( raw_sentence, \
              remove_stopwords ))
    #
    # Return the list of sentences (each sentence is a list of words,
    # so this returns a list of lists
    return sentences

def get_word2vec_model(document):
    sentences = []
    for doc in document:
        doc= filter(lambda x: x in string.printable, doc)
        sentences += doc_to_sentences(doc, tokenizer)

    num_features = 300    # Word vector dimensionality
    min_word_count = 10   # Minimum word count
    num_workers = 2       # Number of threads to run in parallel
    context = 10          # Context window size
    downsampling = 1e-3   # Downsample setting for frequent words
    print "Training model..."
    model = word2vec.Word2Vec(sentences, workers=num_workers, size=num_features, min_count = min_word_count,window = context, sample = downsampling)
    return model


def gen_word2vec_model(document,out_loc):
    sentences = []
    for doc in document:
        doc= filter(lambda x: x in string.printable, doc)
        sentences += doc_to_sentences(doc, tokenizer)

    num_features = 300    # Word vector dimensionality
    min_word_count = 10   # Minimum word count
    num_workers = 2       # Number of threads to run in parallel
    context = 10          # Context window size
    downsampling = 1e-3   # Downsample setting for frequent words
    print "Training model..."
    model = word2vec.Word2Vec(sentences, workers=num_workers, size=num_features, min_count = min_word_count,window = context, sample = downsampling)
    model.save(out_loc)
    print 'model saved'

def gen_word_centroid_map(model_loc,out_loc,words_in_cluster=5):
    model = word2vec.Word2Vec.load(model_loc)
    word_vectors = model.syn0
    num_clusters = model.syn0.shape[0] / words_in_cluster
    kmeans_clustering = KMeans( n_clusters = num_clusters )
    print 'clustering started'
    idx = kmeans_clustering.fit_predict( word_vectors )
    print 'clustering done'
    word_centroid_map = dict(zip( model.index2word, idx ))
    loc = open(out_loc,'w')
    pickle.dump(word_centroid_map,loc)
    loc.close()


def create_bag_of_centroids( wordlist, word_centroid_map ):
    #
    # The number of clusters is equal to the highest cluster index
    # in the word / centroid map
    num_centroids = max( word_centroid_map.values() ) + 1
    #
    # Pre-allocate the bag of centroids vector (for speed)
    bag_of_centroids = np.zeros( num_centroids, dtype="float32" )
    #
    # Loop over the words in the review. If the word is in the vocabulary,
    # find which cluster it belongs to, and increment that cluster count
    # by one
    for word in wordlist:
        if word in word_centroid_map:
            index = word_centroid_map[word]
            bag_of_centroids[index] += 1
    #
    # Return the "bag of centroids"
    return bag_of_centroids

def gen_word2vec_centroids(document,word_centroid_map,num_clusters):
    """
    @params document: np array/pandas column of sentences/paragraphs"""
    # Pre-allocate an array for the training set bags of centroids (for speed)
    document_centroids = np.zeros( (len(document), num_clusters), dtype="float32" )

    clean_document = []
    for doc in document:
        clean_document.append( doc_to_wordlist( doc, remove_stopwords=True ))

    # Transform the training set reviews into bags of centroids
    counter = 0
    for doc in clean_document:
        document_centroids[counter] = create_bag_of_centroids( doc, word_centroid_map )
        counter += 1
    return document_centroids

def get_no_clusters(model_loc,words_in_cluster=5):
    model = word2vec.Word2Vec.load(model_loc)
    num_clusters = model.syn0.shape[0] / words_in_cluster
    return num_clusters

def create_files(document,model_loc,word_centr_map_loc,words_in_cluster=5):
    ''' document is a np/pandas array, out_loc form /home/Desktop/ '''
    print 'start'
    gen_word2vec_model(document,model_loc)
    gen_word_centroid_map(model_loc,word_centr_map_loc,words_in_cluster=words_in_cluster)

def get_doc_clusters(document,model_loc,word_centr_map_loc,words_in_cluster=5):
    loc = open(word_centr_map_loc, 'rb')
    word_centroid_map = pickle.load(loc)
    loc.close()
    num_clusters = get_no_clusters(model_loc,words_in_cluster)
    doc_centroids = gen_word2vec_centroids(document,word_centroid_map,num_clusters)
    return doc_centroids

