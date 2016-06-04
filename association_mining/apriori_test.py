__author__ = 'joswin'
'''
create table apriori_supports (
    items TEXT[] ,
    support float
);
create table apriori_rules (
    items TEXT[] ,
    associated_items TEXT[],
    confidence float,
    lift float
);
'''