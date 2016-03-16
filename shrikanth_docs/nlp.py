import csv
import nltk
from nltk import sent_tokenize, word_tokenize, pos_tag
import re
#from numpy import*
import datetime
import pprint
from nltk import stem
lemmatizer = nltk.stem.wordnet.WordNetLemmatizer() 	
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.snowball import SnowballStemmer
lmtzr = WordNetLemmatizer()
#from nltk import PorterStemmer
snowball_stemmer = SnowballStemmer("english")
porter = stem.porter.PorterStemmer()
lancaster = stem.lancaster.LancasterStemmer()
snowball = stem.snowball.EnglishStemmer()
t1=datetime.datetime.today()
data={}
deal={}	

###########################################################################(data input)

deal_column=raw_input('Enter Deal_name Column:')
incident_id_column=raw_input('Enter Incident_id Column:')
data_column=raw_input('Enter Brief_Description Column:')
filename='Zebradata.csv'

############################################################################(reading data)

with open(filename,'rb') as fp:
	reader=csv.reader(fp)																																						
	next(reader)
	for line in reader:
		data[line[int(incident_id_column)-1]]=[line[int(data_column)-1]]
		deal[line[int(incident_id_column)-1]]=line[int(deal_column)-1]
#data=array(data)

#####################################################(tree navigation)

def tree_travel_1(tree_obj_1):
	global tree_data_1
	global stopwords
	for e in list(tree_obj_1):
		if isinstance(e,nltk.tree.Tree):
			return tree_travel_1(e)
		else:
			if e[0] not in stopwords:
				tree_data_1+=[e[0]]	
				
############################################

def tree_travel(tree_obj):
	global tree_data
	global stopwords
	global final_data_person
	for e in list(tree_obj):
		if isinstance(e,nltk.tree.Tree):
			return tree_travel(e)
		else:
			if e[0] not in stopwords and final_data_person:
				tree_data+=[e[0]]
				
###############################################################################################(date removal)

month_new=[datetime.datetime.strptime(str(i),'%m').strftime('%b') for i in range(1,13)]
month=[datetime.datetime.strptime(str(i),'%m').strftime('%B') for i in range(1,13)]
day=[datetime.datetime.strptime(str(i),'%m').strftime('%A') for i in range(1,8)]
all_month_day=[]
for temp in day:
	for temp1 in month:
		all_month_day+=[temp+', '+temp1]

###############################################################################################(reading files)
stopwords=[]
with open('mystopwords.csv','rb') as fp:
	reader = csv.reader(fp)
	for line in reader:
		stopwords+=line


#abber={}
#with open('abv.csv','rb') as fp:
#	reader=csv.reader(fp)
#	next(reader)
#	for line in reader:
#		abber[line[0].lstrip()]=line[1].lstrip()

con={}
with open('contraction.csv','rb') as fp:
	reader=csv.reader(fp)
	next(reader)
	for line in reader:
		con[line[0].lstrip()]=line[1].lstrip()

################################################################################################

output_phrase={}
final_string={}
noun_phrases={}
verb_phrases={}
person={}
final_data_person=[]
final_data=[]
final_data_different=[]
for key1 in data:

	data_new=[]
	for temp in data[key1]:
		data_new+=[t.strip() for t in re.split('Subject|Status Note|Work Log|-{4,}|_{4,}|={4,}',temp)]
	
	
	data_new1=[]
	for temp in data_new:
		a=re.sub('-{4,}|_{4,}|={4,}',' ',temp)

		for temp1 in all_month_day:
			if temp1 in a:
				a = re.sub(temp1+' [0-9]+','',a)
		
		for temp1 in month_new:
			if temp1 in a:
				a = re.sub(temp1+' +[0-9]+, +[0-9]+','',a)
				a = re.sub(temp1+' [0-9]+','',a)

	########################################################################(removing signatures and from to etc)
	
		a=re.sub(r'From:[\s\w\W]+Subject:','',a)
		a=re.sub(r'Thank you,[\s\w\W]+Subject:','',a)
		a=re.sub(r'Thank you.[\s\w\W]+Subject:','',a)
		a=re.sub(r'Thank you [\s\w\W]+Subject:','',a)
		a=re.sub(r'Thanks,[\s\w\W]+Subject:','',a)
		a=re.sub(r'Thanks.[\s\w\W]+Subject:','',a)
		a=re.sub(r'Thanks [\s\w\W]+Subject:','',a)
		a=re.sub(r'Regards,[\s\w\W]+Subject:','',a)
		a=re.sub(r'Regards.[\s\w\W]+Subject:','',a)
		a=re.sub(r'Regards [\s\w\W]+Subject:','',a)
		a=re.sub(r'Thank you,[\s\w\W]+','',a)#EOT
		a=re.sub(r'Thank you.[\s\w\W]+','',a)#EOT
		a=re.sub(r'Thank you [\s\w\W]+','',a)#EOT
		a=re.sub(r'Thanks,[\s\w\W]+','',a)#EOT
		a=re.sub(r'Thanks.[\s\w\W]+','',a)#EOT
		a=re.sub(r'Thanks [\s\w\W]+','',a)#EOT
		a=re.sub(r'Regards,[\s\w\W]+','',a)#EOT
		a=re.sub(r'Regards.[\s\w\W]+','',a)#EOT
		a=re.sub(r'Regards [\s\w\W]+','',a)#EOT
		
#############################################################(time removal)

		a=re.sub(r'[0-9]+:[0-9]+ AM','',a)
		a=re.sub(r'[0-9]+:[0-9]+ PM','',a)
		
#############################################################(email removal)

		a=re.sub(r'[\w\.-]+@[\w\.-]+',' ',a)
		
#########################################################################(city_state_zip removal)	

		a=re.sub(r'[a-zA-Z]+, [a-zA-Z]{2} - ([0-9]{5} | [0-9]{9})','',a)
		
#########################################################################(phonenumber removal)

		a=re.sub(r'[0-9]+-[0-9]+-[0-9]+','',a)
		a=re.sub(r'([0-9]+) [0-9]+-[0-9]+','',a)
		
		a=re.sub(r'[0-9]+-[0-9]+','',a)
		a=re.sub(r'[0-9]+.[0-9]+.[0-9]+','',a)
		
############################################################################(url removal)

		a=re.sub(r'^https?:\/\/.*[\r\n]*', '', a, flags=re.MULTILINE)
		a=re.sub(r'^http?:\/\/.*[\r\n]*', '', a, flags=re.MULTILINE)
		
############################################################################(numbers removal)

		a=re.sub('(^| )([0-9]+)( |$)',' ',a)
		
#######################################################################################(special characters removal)

		a=re.sub(r'[^a-zA-Z0-9"_"," "]', '', a)
		
#######################################################################################(extra white spaces removal)		

		a=re.sub(r' +',' ',a)
		
		data_new1+=[a]
		
########################################################################################(removing title name)

	data_new5=[]
	pattern=['Dr.','Mr.','Mrs.','Ms.']
	for temp in data_new1:
		for temp1 in pattern:
			if temp1 in temp:
				temp = re.sub(temp1+' [a-zA-Z0-9]+','',temp)
		data_new5+=[temp]
			
	data_new6=[]
	for temp in data_new5:
		data_new6+=[re.sub(r' [0-9]+ ','',temp)]

#################################################################(working on abber and con) 

	data_new3=[]
	for temp in data_new6:
		#for key in abber:
			#temp=re.sub('\\b'+key+' \\b',abber[key]+' ',temp)
			#temp=re.sub('\\b'+key+'\\b',abber[key]+' ',temp)
		for key in con:
			temp=re.sub('\\b'+key+'\\b',con[key],temp)
		data_new3+=[temp]

##################################################################(comma spacer)

	data_new4=[]
	for temp in data_new3:
		strtemp=''
		for temp1 in temp.split(','):
			strtemp+=temp1+', '
		data_new4+=[strtemp]	

	#################################################
	
	print (key1)
	temp_string=''.join(data_new4)
	final_string[key1]=temp_string

#####################################################(tokenization and part of speech tagging)

	sents = sent_tokenize(temp_string)
	tokens = word_tokenize(temp_string)
	#tokens_stem=[porter.stem(i) for i in tokens]
	#tokens_stem=[snowball_stemmer.stem(i) for i in tokens]
	#print(tokens_stem)
	#tokens_stem=[wnl.lemmatize(i) for i in tokens]
	#wordnet_tag ={'NN':'n', 'NNS':'n', 'NNP':'n', 'NNPS':'n','JJ':'a','JJR':'a', 'JJS':'a','VB':'v', 'VBD':'v', 'VBG':'v', 'VBN':'v', 'VBP':'v', 'VBZ':'v','RB':'r','RBR':'r', 'RBS':'r'}
	def get_wordnet_pos(pos_tag):
		if pos_tag[1].startswith('J'):
			return (pos_tag[0], wordnet.ADJ)
		elif pos_tag[1].startswith('V'):
			return (pos_tag[0], wordnet.VERB)
		elif pos_tag[1].startswith('N'):
			return (pos_tag[0], wordnet.NOUN)
		elif pos_tag[1].startswith('R'):
			return (pos_tag[0], wordnet.ADV)
		else:
			return (pos_tag[0], wordnet.NOUN)
	
	
	lemma=[]
	lemma1=[]
	lemma_data=[]
	lemma_data1=[]
	tagged_sent = pos_tag(tokens)
	#print(tagged_sent)
	#pos_a = map(get_wordnet_pos, tokens)
	#lemmae_a = [lemmatizer.lemmatize(token.lower().strip(string.punctuation), pos) for token, pos in pos_a \
    #               if token.lower().strip(string.punctuation) not in stopwords]
	#print(tagged_sent)
	for t in tagged_sent:
		#print t[0],t[1][:1]
		try:
			#print t[0],":",lmtzr.lemmatize(t[0],wordnet_tag[t[1][:2]])
			lemma+=[(lmtzr.lemmatize(t[0],get_wordnet_pos[t[1][:2]]))]
			#lemma+=''.join(e for e in lemma)
			#print(lemma)
			#for t in lemma:
			#	if len(t)>0:
			#		lemma_data+=lemma
			
		except:
			#print t[0],":",lmtzr.lemmatize(t[0])
			lemma+=[(lmtzr.lemmatize(t[0]))]
			#lemma+=''.join(e for e in lemma)
			#print(lemma)
			#lemma+=' '.join(e for e in lemma)
			#print(lemma)
			#for t in lemma1:
			#	if len(t)>0:
			#		lemma_data+=lemma1
	
	#for word, tag in tagged_sent:
	#	wntag = tag[0].lower()
		#print(word, wntag)
	#	wntag = wntag if wntag in ['a', 'r', 'n', 'v'] else None
	#	if not wntag:
	#			lemma+ = word
	#	else:
	#			lemma+ = (wnl.lemmatize(word, wntag),word)
				#print lemma
	#print(tagged_sent)
	#print(lemma)
	#tokens_lemma = word_tokenize(lemma)
	#print(tokens_lemma)
	tagged_sent_lemma=pos_tag(lemma)
	#print(tagged_sent_lemma)
	#tagged_sent = pos_tag(tokens_stem)
	tagged_sent_new=nltk.ne_chunk(tagged_sent_lemma)
	#print(tagged_sent_new)
	

#######################################################################(defining grammar)

	tagged = tagged_sent_new

	grammar = r"""
	NP: {<DT|JJ|NN.*>+}          # Chunk sequences of DT, JJ, NN
	PP: {<IN><NP>}               # Chunk prepositions followed by NP
	VP: {<VB.*><NP|PP>+} # Chunk verbs and their arguments
	CLAUSE: {<NP><VP>}           # Chunk NP, VP
  	"""
	
#######################################################################(Regular expression based Parsing)

	cp = nltk.RegexpParser(grammar)
	
#######################################################################(output parse tree)

	result = cp.parse(tagged)
	#print(result)
	
	output_phrase[key1]=result

###############################################################(named entity recognition)

	temp_list=[]
	for subtree in result.subtrees():
		if subtree.label()=='PERSON':
			tree_data_1=[]
			tree_travel_1(subtree)
			temp_list+=[' '.join((e for e in list(tree_data_1)))]
			
	person[key1]=temp_list
	#print(person[key1])
#################################################################

	for t in person[key1]:
		if len(t)>0:
			final_data_person+=[[t]]
		
#################################################################print("\n# Print noun phrases only")

	temp_list=[]
	for subtree in result.subtrees():
		if subtree.label()=='NP':
			tree_data=[]
			tree_travel(subtree)
			temp_list+=[' '.join((e for e in list(tree_data)))]
			#print(temp_list)
	#str1 = ''.join(str(e) for e in temp_list)		
	#tokens_lemma = word_tokenize(str1)
	#tagged_sent_lemma = pos_tag(tokens_lemma)	
	#print(tagged_sent_lemma)
	#print(tokens_lemma)	
	#print(temp_list)
	#for word, tag in tagged_sent_lemma:
	#	wntag = tag[0].lower()
	#	#print(word, wntag)
	#	wntag = wntag if wntag in ['a', 'r', 'n', 'v'] else None
	#	if not wntag:
	#			lemma = word
	#	else:
	#			lemma = wnl.lemmatize(word, wntag)
		#print(lemma)
	#temp_list+=[' '.join((e for e in list(lemma)))]
	noun_phrases[key1]=temp_list

###############################################################print("\n# Print verb phrases only")

	temp_list=[]
	for subtree in result.subtrees():
		if subtree.label()=='VP':
			tree_data=[]
			tree_travel(subtree)
			temp_list+=[' '.join((e for e in list(tree_data)))]
			#print(temp_list)
	#str2 = ''.join(str(e) for e in temp_list)		
	#tokens_lemma2 = word_tokenize(str2)
	#tagged_sent_lemma2 = pos_tag(tokens_lemma2)	
	#print(tagged_sent_lemma)
	#print(tokens_lemma)	
	#print(temp_list)
	#for word, tag in tagged_sent_lemma2:
	#	wntag = tag[0].lower()
	#	print(word, wntag)
	#	wntag = wntag if wntag in ['a', 'r', 'n', 'v'] else None
	#	if not wntag:
	#			lemma = word
	#	else:
	#			lemma = wnl.lemmatize(word, wntag)
		#print(lemma)		

	#temp_list+=[' '.join((e for e in list(lemma)))]
	verb_phrases[key1]=temp_list
	
##############################################################################################################################	

	for t in noun_phrases[key1]:
		if len(t)>0:
			final_data+=[[deal[key1],key1,t]]
	
	for t in verb_phrases[key1]:
		if len(t)>0:
			final_data+=[[deal[key1],key1,t]]
			
#	final_data+=[[key1,'%'.join([e for e in noun_phrases[key1]]), '%'.join([e for e in verb_phrases[key1]])]]
###############################################################################################################################(getting time)

t2=datetime.datetime.today()
print ('Running Time: %s',str((t2-t1).seconds))

##############################################################################################################(writing data to file)

with open('Zebradata_lemma_output1.csv','wb') as fp:
	writer=csv.writer(fp)
	#final_data.insert(0,['Incident_id','Noun_Phrases','Verb_Phrases'])
	writer.writerows(final_data)