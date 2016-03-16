from entity_extraction import StanfordNERTaggerExtractor,JobsExtractor
from matcher_funs import Matcher
import sys

def identify_jobs_names(text):
    se = StanfordNERTaggerExtractor()
    je = JobsExtractor()
    text_tagged = se.tag_text_multi(text)
    names = se.identify_NER_tags_multi(text_tagged,'PERSON')
    locations = se.identify_NER_tags_multi(text_tagged,'LOCATION')
    orgs = se.identify_NER_tags_multi(text_tagged,'ORGANIZATION')
    jobs = je.find_all_jobs(text)
    names_jobs = Matcher.match_names_and_jobs(text,names,jobs)
    print('names:')
    print(names)
    print('locations:')
    print(locations)
    print('organizations:')
    print(orgs)
    print('jobs:')
    print(jobs)
    print('names and jobs:')
    print(names_jobs)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        text = sys.argv[1]
    else:
        text = '''Mark Zuckerberg has now surpassed Amazon CEO Jeff Bezos to become the fifth richest man in the world. Facebook's main man is now worth an eye-popping $50 billion according to Forbes. The 31-year-old billionaire is now in touching distance of Carlos Slim who was the richest in the world for three years running between 2010 and 2013. The Mexican business magnate is valued at $50.2 billion. Zuckerberg's rise in wealth has been thanks to a Facebook's rise on the Wall Street. Their stocks have risen by over 21% since the report was released on January 27th. In other news, Bill Gates is again the world's richest man with the Microsoft founder and philanthropist being worth an unimaginable $75.6 billion. Amancia Ortega, the owner of one of the most well-known fashion labels in the world, Zara, comes in second at $70 billion. Warren Buffet is third with $59.5 billion. Only three Indians made it to the list of Forbes top 50 richest in the world. Mukesh Ambani found a spot at 27th with $24.8 billion, Azim Premji of Wipro at 43rd with $16.5 billion and Dilip Shanghvi of Sun Pharma following him closely at 44th with $16.4 billion. '''
    identify_jobs_names(text)


