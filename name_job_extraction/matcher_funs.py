import re
from sets import Set

DEBUG = False

def parse_string(x) :
    '''
    :param x:
    :return:
    '''
    t = type(x)
    if t is unicode :
        return x.encode("utf8")
    elif t is str :
        return x
    else :
        raise Exception()

class Matcher:

    @staticmethod
    def match_names_and_jobs( text, names, jobs):
        '''
        :param text: text
        :param names: list of names
        :param jobs: list of jobs
        :return: name and job matches
        '''
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
                    distance = name_start_index - job_end_index
                else:
                    distance = job_start_index - name_end_idex
                assert distance is not None
                distances.append(distance)
                if DEBUG:
                    print "Name: %s, Job: %s, Distance %s" % (
                        name, job, distance
                    )
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
        while (min_distance <= 4*first_min_distance or min_distance > 10) \
              and min_distance <= 50 \
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
        if DEBUG:
            print "*" * 100
        return job_and_names
