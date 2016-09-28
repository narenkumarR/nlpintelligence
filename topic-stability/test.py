__author__ = 'madan'
'''
python parse-text.py data/saas_companies/ -o saas
python reference-nmf.py saas.pkl --kmin 10 --kmax 100 -o reference-nmf-saas/
python generate-nmf.py saas.pkl --kmin 10 --kmax 100 -r 20 -o topic-nmf-saas/


running:

import unsupervised.rankings
metric = unsupervised.rankings.AverageJaccard()
matcher = unsupervised.rankings.RankingSetAgreement( metric )

ranking1 = [[1wrd11,1wrd12,1wrd13,..,1wrd1t],[1wrd21,1wrd22,..,1wrd2t],..,[1wrdn1,1wrdn2,..,1wrdnt]]
ranking2 = [[2wrd11,2wrd12,2wrd13,..,2wrd1t],[2wrd21,2wrd22,..,2wrd2t],..,[2wrdn1,2wrdn2,..,2wrdnt]]
score = matcher.similarity( ranking1,ranking2 )
# to average, take ranking1 as base_ranking, using entire dataset
#ranking2 is constructed from an 80% sample. calculate score for multiple samples and take average.
'''