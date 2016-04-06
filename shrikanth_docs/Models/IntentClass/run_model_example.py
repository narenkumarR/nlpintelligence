__author__ = 'joswin'

# Required packages:
# nltk, sklearn, pickle, quotequail, re

from intent_class_models.naive_bayes.naive_bayes_model import NaiveBayesModel
nb = NaiveBayesModel()
nb.load_model()
nb.predict_textinput('Thank you but not interested')
