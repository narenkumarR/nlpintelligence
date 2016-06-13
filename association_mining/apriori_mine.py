"""
Description     : Simple Python implementation of the Apriori Algorithm

Usage:
    $python apriori.py -f DATASET.csv -s minSupport  -c minConfidence

    $python apriori.py -f DATASET.csv -s 0.15 -c 0.6
"""

import sys
import pdb
import logging
import pickle
import gc
# logging.basicConfig(filename='new algo.log', level=logging.INFO)

import psycopg2

from itertools import chain, combinations
from collections import defaultdict
from optparse import OptionParser

def chunk(l, n):
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]


class Apriori(object):
    def __init__(self,use_db=False,query=None,use_iter=False):
        '''
        :param use_db: flag to use db as input or file
        :param query: query for selecting the data
        :param iter: to use iterator based calculation. will be useful if the data is very large to fit in memory,not implemented now. 
                        only iterator based support present now
        :return:
        '''
        self.use_db = use_db
        self.use_iter = use_iter
        if use_db:
            self.con = psycopg2.connect(database='builtwith_data', user='postgres',password='postgres',host='localhost')
            self.cursor = self.con.cursor()
            self.query = query


    def subsets(self,arr):
        """ Returns non empty subsets of arr"""
        return chain(*[combinations(arr, i + 1) for i, a in enumerate(arr)])


    def returnItemsWithMinSupport(self,itemSet, transactionList, minSupport, freqSet):
            """calculates the support for items in the itemSet and returns a subset
           of the itemSet each of whose elements satisfies the minimum support"""
            _itemSet = set()
            localSet = defaultdict(int)

            for transaction in transactionList:
                # logging.info('transaction:{}'.format(transaction))
                for item in itemSet:
                    # logging.info('item:{}'.format(item))
                    if item.issubset(transaction):
                            freqSet[item] += 1
                            localSet[item] += 1
            for item, count in localSet.items():
                support = float(count)/self.no_records

                if support >= minSupport:
                        _itemSet.add(item)

            return _itemSet


    def joinSet(self,itemSet, length):
            """Join a set with itself and returns the n-element itemsets"""
            return set([i.union(j) for i in itemSet for j in itemSet if len(i.union(j)) == length])

    def dataFromFile(self,fname):
            """Function which reads from the file and yields a generator"""
            file_iter = open(fname, 'rU')
            for line in file_iter:
                    line = line.strip().rstrip(',')                         # Remove trailing comma
                    record = frozenset(line.split(','))
                    yield record

    def data_from_postgres(self,query):
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        while row:
            yield frozenset(row[0])
            row = self.cursor.fetchone()

    def getItemSetTransactionList(self,data_iterator):
        transactionList = list()
        itemSet = set()
        for record in data_iterator:
            transaction = frozenset(record)
            transactionList.append(transaction)
            for item in transaction:
                itemSet.add(frozenset([item]))              # Generate 1-itemSets
        return itemSet, transactionList

    def getItemSet_file_input(self,inFile):
        data_iterator = self.dataFromFile(inFile)
        itemSet = set()
        self.no_records = 0
        for record in data_iterator:
            self.no_records += 1
            transaction = frozenset(record)
            for item in transaction:
                itemSet.add(frozenset([item]))              # Generate 1-itemSets
        return itemSet

    def getItemSet_postgres(self,query):
        data_iterator = self.data_from_postgres(query)
        itemSet = set()
        self.no_records = 0
        for record in data_iterator:
            self.no_records += 1
            transaction = frozenset(record)
            for item in transaction:
                itemSet.add(frozenset([item]))              # Generate 1-itemSets
        return itemSet
    def getTransactionListIter_file_input(self,inFile):
        data_iterator = self.dataFromFile(inFile)
        for record in data_iterator:
            transaction = frozenset(record)
            yield transaction

    def getTransactionListIter_postgres(self,query):
        data_iterator = self.data_from_postgres(query)
        for record in data_iterator:
            transaction = frozenset(record)
            yield transaction

    def getItemsetTransactionList_postgres(self,query):
        data_iterator = self.data_from_postgres(query)
        itemSet = set()
        self.no_records = 0
        transactions = []
        for record in data_iterator:
            self.no_records += 1
            transaction = frozenset(record)
            transactions.append(transaction)
            for item in transaction:
                itemSet.add(frozenset([item]))              # Generate 1-itemSets
        return itemSet,transactions

    def runApriori(self,input_str, minSupport, minConfidence):
        """
        run the apriori algorithm. data_iter is a record iterator
        Return both:
         - items (tuple, support)
         - rules ((pretuple, posttuple), confidence)
        """
        # itemSet, transactionList = getItemSetTransactionList(data_iter)
        if self.use_db:
            if self.use_iter:
                itemSet,transactionList = self.getItemsetTransactionList_postgres(self.query)
                print(len(itemSet))
            else:
                itemSet = self.getItemSet_postgres(self.query)
                print(len(itemSet))
                transactionList = self.getTransactionListIter_postgres(self.query)
        else:
            itemSet = self.getItemSet_file_input(input_str)
            print(len(itemSet))
            transactionList = self.getTransactionListIter_file_input(input_str)

        freqSet = defaultdict(int)
        largeSet = dict()
        # Global dictionary which stores (key=n-itemSets,value=support)
        # which satisfy minSupport

        assocRules = dict()
        # Dictionary which stores Association Rules

        oneCSet = self.returnItemsWithMinSupport(itemSet,
                                            transactionList,
                                            minSupport,
                                            freqSet)

        currentLSet = oneCSet
        k = 2
        while(currentLSet != set([])):
            if self.use_db:
                transactionList = self.getTransactionListIter_postgres(self.query)
            else:
                transactionList = self.getTransactionListIter_file_input(input_str)
            largeSet[k-1] = currentLSet
            currentLSet = self.joinSet(currentLSet, k)
            currentCSet = self.returnItemsWithMinSupport(currentLSet,
                                                    transactionList,
                                                    minSupport,
                                                    freqSet)
            currentLSet = currentCSet
            k = k + 1

        def getSupport(item):
                """local function which Returns the support of an item"""
                return float(freqSet[item])/self.no_records

        toRetItems = []
        for key, value in largeSet.items():
            toRetItems.extend([(tuple(item), getSupport(item))
                               for item in value])

        toRetRules = []
        for key, value in largeSet.items()[1:]:
            for item in value:
                _subsets = map(frozenset, [x for x in self.subsets(item)])
                for element in _subsets:
                    remain = item.difference(element)
                    if len(remain) > 0:
                        confidence = getSupport(item)/getSupport(element)
                        if confidence >= minConfidence:
                            lift = confidence/getSupport(remain)
                            toRetRules.append(((tuple(element), tuple(remain)),
                                               confidence,lift))
        return toRetItems, toRetRules


    def printResults(self,items, rules):
        """prints the generated itemsets sorted by support and the confidence rules sorted by confidence"""
        for item, support in sorted(items, key=lambda (item, support): support):
            print "item: %s , %.3f" % (str(item), support)
        print "\n------------------------ RULES:"
        for rule, confidence in sorted(rules, key=lambda (rule, confidence): confidence):
            pre, post = rule
            print "Rule: %s ==> %s , %.3f" % (str(pre), str(post), confidence)

    def save_results_postgres(self,items,rules):
        self.cursor.execute('drop table if exists apriori_supports')
        self.cursor.execute('drop table if exists apriori_rules')
        self.cursor.execute('create table apriori_supports (items TEXT[],support float)')
        self.cursor.execute('create table apriori_rules (items TEXT[],associated_items TEXT[],confidence float,lift float)')
        self.con.commit()
        items = [(list(item),support) for item,support in items]
        rules = [(list(associated_items[0]),list(associated_items[1]),confidence,lift) for associated_items,confidence,lift in rules]
        gc.collect()
        for chuk in chunk(items,10000):
            records_list_template = ','.join(['%s'] * len(chuk))
            insert_query = 'insert into apriori_supports (items,support) values {0}'.format(records_list_template)
            self.cursor.execute(insert_query,chuk)
            self.con.commit()
        # insert_query = 'insert into apriori_supports (items,support) values (%s,%s)'
        # for item,support in items:
        #     self.cursor.execute(insert_query,(list(item),support))
        # records_list_template = ','.join(['%s'] * len(items))
        # insert_query = 'insert into apriori_rules (items,associated_items,confidence,lif) values {0}'.format(records_list_template)
        # self.cursor.execute(insert_query,rules)
        # insert_query = 'insert into apriori_rules (items,associated_items,confidence,lift) values (%s,%s,%s,%s)'
        # for associated_items,confidence,lift in rules:
        #     item_list = list(associated_items[0])
        #     asso_list = list(associated_items[1])
        #     self.cursor.execute(insert_query,(item_list,asso_list,confidence,lift))
        for chuk in chunk(rules,10000):
            records_list_template = ','.join(['%s'] * len(chuk))
            insert_query = 'insert into apriori_rules (items,associated_items,confidence,lift) values {0}'.format(records_list_template)
            self.cursor.execute(insert_query,chuk)
            self.con.commit()
        self.cursor.execute('CREATE INDEX item_index on "apriori_supports" USING GIN ("items")')
        self.cursor.execute('CREATE INDEX item_rule_index on "apriori_rules" USING GIN ("items","associated_items")')
        self.con.commit()

    def save_results_file(self,items,rules):
        with open('items.pkl','w') as f:
            pickle.dump(items,f)
        with open('rules.pkl','w') as f:
            pickle.dump(rules,f)

    def run_apriori_db(self,minSupport=0.15, minConfidence=0.6):
        '''
        :param minSupport:
        :param minConfidence:
        :return:
        '''
        items,rules = self.runApriori('',minSupport,minConfidence)
        self.save_results_postgres(items,rules)


if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-f', '--inputString',
                         dest='input',
                         help='filename containing csv or input query',
                         default=None)
    optparser.add_option('-s', '--minSupport',
                         dest='minS',
                         help='minimum support value',
                         default=0.15,
                         type='float')
    optparser.add_option('-c', '--minConfidence',
                         dest='minC',
                         help='minimum confidence value',
                         default=0.6,
                         type='float')
    optparser.add_option('-d', '--useDB',
                         dest='use_db',
                         help='use db',
                         default=0,
                         type='float')

    (options, args) = optparser.parse_args()

    # inFile = None
    # if options.input is None:
    #         inFile = sys.stdin
    # elif options.input is not None:
    #         inFile = dataFromFile(options.input)
    # else:
    #         print 'No dataset filename specified, system with exit\n'
    #         sys.exit('System will exit')

    minSupport = options.minS
    minConfidence = options.minC
    inFile = options.input

    apriori_class = Apriori()
    items, rules = apriori_class.runApriori(inFile, minSupport, minConfidence)

    apriori_class.printResults(items, rules)
