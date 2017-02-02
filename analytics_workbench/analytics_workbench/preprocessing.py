__author__ = 'joswin'
# -*- coding: utf-8 -*-

import pandas as pd, numpy as np

from scipy.sparse import hstack
from scipy.sparse.csr import csr_matrix
from sklearn import preprocessing

from .process_text import ProcessText

class PreProcessor(object):
    ''' 
    '''
    def __init__(self):
        self.text_processor = ProcessText()
    
    def build_matrix_train(self,df,text_cols=[],categorical_cols=[],numeric_cols=[],dv_col=None,vectorizer_type='Count',synonym_loc=None,
                   stem_type='lemmatize',phrase_generation=False,stop_words_loc=None,lower=True,n_gram_range=(1,2),
                   max_df=0.9,min_df=0.01,vocabulary_loc=None,sparse_format=True,scaling='max_abs_scaling',
                   **text_processing_args):
        ''' text columns will be used to create document term matrix, category_cols will be converted to binary .
           this will be used on training set. use get_matrix_test function for test/validation data'''
        self.sparse_format = sparse_format
        self.scaler = self.gen_scaler_object(scaling)
        self.all_cols = df.columns
        if dv_col and dv_col in df.columns:
            df.drop(dv_col, axis=1, inplace=True)
        self.text_cols = text_cols
        self.categorical_cols = categorical_cols
        self.numeric_cols = numeric_cols
        if not self.text_cols and not self.categorical_cols and not self.numeric_cols:
            raise ValueError('No valid columns provided')
        df_numeric,df_text,df_categorical = self.split_df_by_category(df)
        return self._build_matrix(df_numeric,df_text,df_categorical,vectorizer_type=vectorizer_type,synonym_loc=synonym_loc,
                                                      stem_type=stem_type,phrase_generation=phrase_generation,
                                                      stop_words_loc=stop_words_loc,lower=lower,n_gram_range=n_gram_range,
                                                      max_df=max_df,min_df=min_df,vocabulary_loc=vocabulary_loc,
                                                      **text_processing_args)
    
    def _build_matrix(self,df_numeric,df_text,df_categorical,vectorizer_type='Count',synonym_loc=None,
                   stem_type='lemmatize',phrase_generation=False,stop_words_loc=None,lower=True,n_gram_range=(1,2),
                   max_df=0.9,min_df=0.01,vocabulary_loc=None,**text_processing_args):
        '''build the matrix from input data. this will be used on training set to build the data.
           use transform_input_data function to get the matrix from test/validation data'''
        df_categorical_binary_coded = self.get_categorical_labels_train(df_categorical)
        df_text_dtm,vocabulary = self.process_text_columns_train(df_text,vectorizer_type=vectorizer_type,synonym_loc=synonym_loc,
                                                      stem_type=stem_type,phrase_generation=phrase_generation,
                                                      stop_words_loc=stop_words_loc,lower=lower,n_gram_range=n_gram_range,
                                                      max_df=max_df,min_df=min_df,vocabulary_loc=vocabulary_loc,
                                                      **text_processing_args)
        if self.sparse_format:
            complete_matrix = hstack([csr_matrix(df_numeric.values.astype(float)),
                    csr_matrix(df_categorical_binary_coded.values.astype(float)),df_text_dtm])
        else:
            df_text_dtm = pd.DataFrame(df_text_dtm.toarray(),columns=vocabulary)
            complete_matrix = pd.concat([df_numeric,df_categorical_binary_coded,df_text_dtm],axis=1)
        complete_matrix = self.get_scaled_matrix_train(complete_matrix)
        self.column_names = df_numeric.columns.values.tolist()+df_categorical_binary_coded.columns.values.tolist()+vocabulary
        return complete_matrix,self.column_names

    
    def get_matrix_test(self,df):
        ''' transform the input dataframe using the already built objects on the training data'''
        df_numeric,df_text,df_categorical = self.split_df_by_category(df)
        df_categorical_binary_coded = self.get_categorical_labels_test(df_categorical)
        df_text_dtm,vocabulary = self.process_text_columns_test(df_text)
        if self.sparse_format:
            
            complete_matrix = hstack([csr_matrix(df_numeric.values.astype(float)),
                    csr_matrix(df_categorical_binary_coded.values.astype(float)),df_text_dtm])
        else:
            df_text_dtm = pd.DataFrame(df_text_dtm.toarray(),columns=vocabulary)
            complete_matrix = pd.concat([df_numeric,df_categorical_binary_coded,df_text_dtm],axis=1)
        complete_matrix = self.get_scaled_matrix_test(complete_matrix)
        return complete_matrix,self.column_names

    def gen_scaler_object(self,scaling):
        '''
        :param scaling:
        :return:
        '''
        if scaling:
            if scaling == 'max_abs_scaling':#useful for sparse matrix
                scaler = preprocessing.MaxAbsScaler()
            elif scaling == 'standard_scaler':
                if self.sparse_format:
                    raise ValueError('Sparse format not allowed with standard_scaler. Use max_abs_scaling')
                scaler = preprocessing.StandardScaler()
            elif 'sklearn.preprocessing' in str(type(scaling)):
                scaler = scaling #scaling object is passed
            else:
                raise ValueError('Scaling parameter is not passed properly')
            return scaler
        else:
            return False

    def get_scaled_matrix_train(self,inp_matrix):
        '''
        :param inp_matrix:
        :return:
        '''
        if self.scaler:
            if not self.sparse_format and type(inp_matrix) == pd.core.frame.DataFrame:
                cols = inp_matrix.columns
                out_matrix = self.scaler.fit_transform(inp_matrix)
                return pd.DataFrame(out_matrix,columns=cols)
            else:
                return self.scaler.fit_transform(inp_matrix)
        else:
            return inp_matrix

    def get_scaled_matrix_test(self,inp_matrix):
        if self.scaler:
            return self.scaler.transform(inp_matrix)
        else:
            return inp_matrix
    
    def process_text_columns_train(self,df_text,vectorizer_type='Count',synonym_loc=None,stem_type='lemmatize',
                            phrase_generation=False,stop_words_loc=None,lower=True,
                             n_gram_range=(1,2),max_df=0.9,min_df=0.01,vocabulary_loc=None,**text_processing_args):
        ''' this function takes a df with only text columns, merge the columns and returns document term matrix'''
        if df_text.empty:
            return csr_matrix(pd.DataFrame()),[]
        else:
            text_documents = df_text.apply(lambda x: '. '.join(list(x)),1)
            # combine all text columns into a single column
            return self.text_processor.gen_dtm_from_files(text_documents,vectorizer_type=vectorizer_type,synonym_loc=synonym_loc,
                                                          stem_type=stem_type,phrase_generation=phrase_generation,
                                                          stop_words_loc=stop_words_loc,lower=lower,n_gram_range=n_gram_range,
                                                          max_df=max_df,min_df=min_df,vocabulary_loc=vocabulary_loc,
                                                          **text_processing_args)
    
    def process_text_columns_test(self,df_text):
        if df_text.empty:
            return pd.DataFrame(),[]
        else:
            text_documents = df_text.apply(lambda x: '. '.join(list(x)),1)
            return self.text_processor.transform_text_list(text_documents)
    
    def get_categorical_labels_train(self,df_categorical):
        ''' use this in training data. use get_categorical_labels for test data '''
        if not self.categorical_cols:
            assert df_categorical.empty
            return df_categorical
        else:
            self.categories_dic = {}
            for col in self.categorical_cols:
                df_categorical[col] = df_categorical[col].astype('category')
                self.categories_dic[col] = df_categorical[col].cat.categories
            return pd.get_dummies(df_categorical)
    
    def get_categorical_labels_test(self,df_categorical):
        if not self.categorical_cols:
            return pd.DataFrame()
        else:
            for col in self.categorical_cols:
                df_categorical[col] = df_categorical[col].astype('category',categories=self.categories_dic[col])
            return pd.get_dummies(df_categorical)
        
    def split_df_by_category(self,df):
        df_text = df[self.text_cols]
        df_categorical = df[self.categorical_cols]
        df_numeric = df[self.numeric_cols]
        return df_numeric,df_text,df_categorical
        