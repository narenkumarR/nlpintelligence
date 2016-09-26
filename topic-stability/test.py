__author__ = 'madan'
'''
python parse-text.py data/saas_companies/ -o saas
python reference-nmf.py saas.pkl --kmin 10 --kmax 100 -o reference-nmf-saas/
python generate-nmf.py saas.pkl --kmin 10 --kmax 100 -r 20 -o topic-nmf-saas/
'''