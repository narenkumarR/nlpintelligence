import re
from sets import Set

<<<<<<< HEAD
import pdb
import sys
import pandas as pd

def parse_string(x) :
    '''
    :param x:
    :return:
    '''
=======
DEBUG = False

def parse_string(x) :
>>>>>>> first commit
    t = type(x)
    if t is unicode :
        return x.encode("utf8")
    elif t is str :
        return x
    else :
<<<<<<< HEAD
        raise Exception()

class Matcher(object):
    def __init__(self):
        pass

    def match_names_and_jobs(self, text, names, jobs):
        '''
        :param text: text
        :param names: list of names
        :param jobs: list of jobs
        :return: name and job matches
        '''
        tmp = pd.read_csv('../constant_files.csv',index_col=0)
        DEBUG = tmp.loc['DEBUG','value']
        if DEBUG:
            orig_stdout = sys.stdout
            f = file('logfile.txt', 'w')
            sys.stdout = f

=======
        raise expectation_error("a string", x)

class Matcher:

    @staticmethod
    def match_names_and_jobs( text, names, jobs):
>>>>>>> first commit
        names = list([x for x in names if x not in jobs])
        names = list(Set([x for x in names]))
        jobs = list(Set([x for x in jobs]))
        expression = '|'.join(
            [re.escape(x) for x in names] +
            [re.escape(x) for x in jobs]
        )
        p = re.compile(expression)
        iterator = p.finditer(text)
        names_dict = {}
        jobs_dict = {}
        for match in iterator:
            match_text = match.group()
            if match_text in names:
                names_dict[match.start()] = (
                    match_text,
                    match.start() + len(match_text)
                )
            elif match_text in jobs:
                jobs_dict[match.start()] = (
                    match_text,
                    match.start() + len(match_text)
                )

        sorted_job_indexes = jobs_dict.keys()
        sorted_job_indexes.sort()
        sorted_name_indexes = names_dict.keys()
        sorted_name_indexes.sort()
        distances = []
        if DEBUG:
            print "*" * 100
            print "jobs_dict"
            print jobs_dict
            print "names_dict"
            print names_dict
        for job_start_index in sorted_job_indexes:
            job_end_index = jobs_dict[job_start_index][1]
            job = jobs_dict[job_start_index][0]
            for name_start_index in sorted_name_indexes:
                name_end_idex = names_dict[name_start_index][1]
                name = names_dict[name_start_index][0]
                distance = None
                if job_start_index < name_start_index:
<<<<<<< HEAD
                    distance = 99999999
=======
                    distance = name_start_index - job_end_index
>>>>>>> first commit
                else:
                    distance = job_start_index - name_end_idex
                assert distance is not None
                distances.append(distance)
                if DEBUG:
                    print "Name: %s, Job: %s, Distance %s" % (
                        name, job, distance
                    )
<<<<<<< HEAD
        job_and_names = self.matching_fun_orig(jobs_dict,names_dict,distances,sorted_job_indexes,sorted_name_indexes,DEBUG)
        if DEBUG:
            print "*" * 100
            sys.stdout = orig_stdout
            f.close()

        return job_and_names

    def matching_fun_orig(self,jobs_dict,names_dict,distances,sorted_job_indexes,sorted_name_indexes,DEBUG=False):
=======
>>>>>>> first commit
        if DEBUG:
            print "*" * 100
        job_and_names = []
        if len(distances) == 0:
            return job_and_names
        max_distance = max(distances)
        min_distance = min(distances)
        first_min_distance = min_distance
        m = len(sorted_job_indexes)
        n = len(sorted_name_indexes)
        if DEBUG:
            print "*" * 100
<<<<<<< HEAD
        while (min_distance <= 4*first_min_distance or min_distance <= first_min_distance+20) \
              and min_distance <= min(100,max(50,first_min_distance+20)) \
                and min_distance <= max_distance:
            ind = distances.index(min_distance)
            j = ind % n #j indicate name position in the sorted_name_indexes list
            i = ind / n #i indicate job position in the sorted_job_indexes list
            if len(sorted_job_indexes) <= i or \
                    len(sorted_name_indexes) <= j:
                print "min_distance", min_distance
                print "ind", ind
                print "n", n
                print "sorted_job_indexes", len(sorted_job_indexes)
                print "sorted_name_indexes", len(sorted_name_indexes)
                print distances
                assert False
            name_index = sorted_name_indexes[j]
            name = names_dict[name_index][0]
            job_index = sorted_job_indexes[i]
            job = jobs_dict[job_index][0]
            if DEBUG:
                print "*" * 100
                print "Name: %s, Job: %s, Distance %s" % (
                    name, job, min_distance
                )
            job_and_names.append((
                parse_string(
                    re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "",  job)
                ),
                parse_string(
                    re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "",  name)
                )
            ))
            # pdb.set_trace()
            # for k in range(0, m):
            #     distances[k*n+j] = max_distance + 1
            for k in range(0, n):
                distances[i*n+k] = max_distance + 1
            min_distance = min(distances)

        return job_and_names

    def matching_fun_bck(self,jobs_dict,names_dict,distances,sorted_job_indexes,sorted_name_indexes,DEBUG=False):
        if DEBUG:
            print "*" * 100
        job_and_names = []
        if len(distances) == 0:
            return job_and_names
        max_distance = max(distances)
        min_distance = min(distances)
        first_min_distance = min_distance
        m = len(sorted_job_indexes)
        n = len(sorted_name_indexes)
        if DEBUG:
            print "*" * 100
        while (min_distance <= 4*first_min_distance ) \
              and min_distance <= 50 \
=======
        while min_distance <= 4*first_min_distance \
                and min_distance <= 50 \
>>>>>>> first commit
                and min_distance <= max_distance:
            ind = distances.index(min_distance)
            j = ind % n
            i = ind / n
            if len(sorted_job_indexes) <= i or \
                    len(sorted_name_indexes) <= j:
                print "min_distance", min_distance
                print "ind", ind
                print "n", n
                print "sorted_job_indexes", len(sorted_job_indexes)
                print "sorted_name_indexes", len(sorted_name_indexes)
                print distances
                assert False
            name_index = sorted_name_indexes[j]
            name = names_dict[name_index][0]
            job_index = sorted_job_indexes[i]
            job = jobs_dict[job_index][0]
            if DEBUG:
                print "Name: %s, Job: %s, Distance %s" % (
                    name, job, min_distance
                )
            job_and_names.append((
                parse_string(
                    re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "",  job)
                ),
                parse_string(
                    re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "",  name)
                )
            ))
            for k in range(0, m):
                distances[k*n+j] = max_distance + 1
            for k in range(0, n):
                distances[i*n+k] = max_distance + 1
            min_distance = min(distances)
<<<<<<< HEAD

=======
        if DEBUG:
            print "*" * 100
>>>>>>> first commit
        return job_and_names
