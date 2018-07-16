from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction import text as sktxt
from nltk.stem.porter import PorterStemmer
from sklearn.decomposition import NMF, LatentDirichletAllocation
from sklearn.preprocessing import normalize
import mysql.connector
import common.config as c
import numpy as np
import pandas as pd
import pickle
import time
import warnings as wrn

DBUSER='bigdataproj'
DBPWD='bigdataproj@123'
DBHOST='n12'
DB='data_AM'
max_keywords = 60000
ngram_range = (1,3)

custom_stopwords = sktxt.ENGLISH_STOP_WORDS.union(set(open("./topicmodel/stopwords.txt").read().split("\n")))

def dbConnect():
	cnx = mysql.connector.connect(user=DBUSER, password=DBPWD,host =DBHOST,database=DB)
	cursor = cnx.cursor()
	return cnx,cursor

def store_in_db(db,lda,topic_paper_dist,papers,tf_feature_names,domain):
	cur = db.cursor()
	cur.execute("CREATE TABLE if not exists TopicKeywords_20k_3gram_"+domain+"_"+str(c.domain_topics[domain])+"(topic_id varchar(255) NOT NULL,keyword varchar(255) NOT NULL, prob FLOAT,  PRIMARY KEY (topic_id, keyword))")
	cur.execute("CREATE TABLE if not exists  DocTopic_20k_3gram_"+domain+"_"+str(c.domain_topics[domain])+"(paper_id varchar(255) NOT NULL,topic_id varchar(255) NOT NULL,prob FLOAT, PRIMARY KEY (paper_id,topic_id))")
	cur.execute("CREATE TABLE if not exists  TopicLabels_20k_3gram_"+domain+"_"+str(c.domain_topics[domain])+"(topic_id varchar(255) NOT NULL,label varchar(255) NOT NULL,PRIMARY KEY (topic_id))")
	
	cur.execute("delete from TopicKeywords_20k_3gram_"+domain+"_"+str(c.domain_topics[domain]))
	cur.execute("delete from DocTopic_20k_3gram_"+domain+"_"+str(c.domain_topics[domain]))
	cur.execute("delete from TopicLabels_20k_3gram_"+domain+"_"+str(c.domain_topics[domain]))	
	f = open(domain+"_auto_labels.csv","w")
	for topic_id,topic_word in enumerate(normalize( lda.components_, norm='l1', axis=1)):
		label = label_topic(tf_feature_names,topic_word,label_ngram=3)
		f.write("Topic_"+str(topic_id)+","+label+",")
		f.write("-".join([tf_feature_names[i] for i in topic_word.argsort()[:-5 - 1:-1]])+"\n")
		cur.execute('insert into TopicLabels_20k_3gram_'+domain+'_'+str(c.domain_topics[domain])+' (topic_id,label) values ("Topic_'+str(topic_id)+'","'+label+'");')
		for i,kw in enumerate(tf_feature_names):
			cur.execute("INSERT INTO TopicKeywords_20k_3gram_"+domain+"_"+str(c.domain_topics[domain])+" (topic_id,keyword,prob) VALUES ('Topic_"+str(topic_id)+"','"+kw+"',"+str(topic_word[i])+");")

	print 'TopicLabels_20k_3gram_'+domain+'_'+str(c.domain_topics[domain])+'   created'
	print 'TopicKeywords_20k_3gram_'+domain+'_'+str(c.domain_topics[domain])+'   created'
	count = 0
	for index,paper_topic in enumerate(topic_paper_dist):
		for topic_id,prob in enumerate(paper_topic):
			if prob >=0.00:
				try:
					cur.execute("insert into DocTopic_20k_3gram_"+domain+"_"+str(c.domain_topics[domain])+" (topic_id,paper_id,prob) values ('Topic_"+str(topic_id)+"','"+str(papers[index])+"',"+str(round(prob,4))+");")
				except:
					count = count + 1
	
	db.commit()
	print "->  duplicate count", count
	print 'DocTopicTopic_3gram_'+domain+'_'+str(c.domain_topics[domain])+'   created'

def label_topic(tf_feature_names,topic_word,label_len=100,label_ngram=3):
		contenders = {}
		for i ,prob in enumerate(topic_word):
			contenders[tf_feature_names[i]] = prob
		top_kws = sorted(contenders, key=contenders.get, reverse=True)
		l = 0
		label = {}
		topic_label = ""
		# To choose only ngram as label
		if label_ngram > 0:
			for kw in top_kws:
				if len(kw.split(" ")) == label_ngram:
					topic_label= kw
					break
			
		'''
		for i in xrange(0,label_len,1):
			label[top_kws[i]]=contenders[top_kws[i]]
		'''
		return topic_label#,label

def remove_non_alpha(s, chars_to_keep={}, lower=False): 
	res = "".join(i for i in s if (i in chars_to_keep or (ord(i)<128 and i.isalpha())))
	if lower: return res.lower() 
	else: return res

def preprocess_abstract(abstracts):
	stemmer = PorterStemmer()
	unstemmed = {}
	clean_abstracts = []
	for e in abstracts:
		l = []
		for word in e.lower().replace("-"," ").split(" "):
			if word not in custom_stopwords and word!="" and len(word) >=4:					  
				word = remove_non_alpha(word)
				try:							
					stemword = stemmer.stem(word)
					#stemword = word
				except IndexError:
					pass
				if stemword not in custom_stopwords:
					l.append(stemword)			  
		clean_abstracts.append(" ".join(l))
	return clean_abstracts, unstemmed

def paper_abstract_sql(domain):
	return "SELECT paper_id,abstract FROM pub_"+domain+" WHERE LENGTH(abstract) > 100 AND pub_year <= 2013"
def paper_abstract_all_sql():
	return "SELECT paper_id,abstract FROM pub_se WHERE LENGTH(abstract) > 100 AND pub_year <= 2013 UNION SELECT paper_id,abstract FROM pub_db WHERE LENGTH(abstract) > 100 AND pub_year <= 2013 UNION SELECT paper_id,abstract FROM pub_os WHERE LENGTH(abstract) > 100 AND pub_year <= 2013 UNION SELECT paper_id,abstract FROM pub_ai WHERE LENGTH(abstract) > 100 AND pub_year <= 2013"

def save_model(lda,domain,num_topics,feature_names):
	with open("lda_"+domain+"_"+c.query_name+"_"+str(num_topics)+".sav","wb") as f:
		pickle.dump(lda,f)
		pickle.dump(feature_names,f)
	print "model saved"

def load_model(domain,num_topics):	
	try:	
		with open("lda_"+domain+"_"+c.query_name+"_"+str(num_topics)+".sav","rb") as f:
			lda = pickle.load(f)
			feature_names = pickle.load(f)
			return lda,feature_names
	except:
		wrn.warn("Unable to load model, save file does not exist or is corrupted!!")	

def load_corpus(domain,db):
	vectorizer = CountVectorizer(max_features=max_keywords,
								stop_words='english', 
								ngram_range = ngram_range, 
								max_df=0.95)
	abstracts = []
	papers = []
	sql = ""
	if domain=="all" or domain[:4]=="soup":
		sql = paper_abstract_all_sql()
	else:
		sql = paper_abstract_sql(domain)

	cursor = db.cursor()
	cursor.execute(sql)
	rows = cursor.fetchall()
	for row in rows:
		abstracts.append(row[1])
		papers.append(row[0])
	clean_abstracts, unstemmed = preprocess_abstract(abstracts)
	print "number of papers of",domain,":",len(clean_abstracts)
	tf = vectorizer.fit_transform(clean_abstracts)
	print "tf shape of",domain,":",tf.shape
	return papers, tf, vectorizer.get_feature_names()

def generate_topics():
	db,cursor = dbConnect()
	for domain in c.domains:
		start_time = time.time()
		papers, tf, feature_names= load_corpus(domain,db)
		#lda,feature_names=load_model(domain,c.domain_topics[domain])
		lda = LatentDirichletAllocation(n_topics=c.domain_topics[domain], 
										max_iter=5,
										learning_method='online',
										learning_offset=50.,
										random_state=0)
		lda.fit(tf)		
		#---------- MODEL EVALUATION PARAMETERS --------------------------
		perplexity1 = lda.perplexity(tf)
		perplexity2 = lda.perplexity(tf,lda._e_step(tf,False,False)[0])
		score = lda.score(tf,lda._e_step(tf,False,False)[0])
		topic_paper_dist = lda.transform(tf)
		print "for",c.domain_topics[domain],domain,"topics ==> perplexity:",perplexity2,"log likelihood:",score
		
		save_model(lda,domain,c.domain_topics[domain],feature_names)
		#lda,feature_names=load_model(domain,c.domain_topics[domain])
		store_in_db(db,lda,topic_paper_dist,papers,feature_names,domain)
		print "--- time for "+domain+": "+str((time.time() - start_time)/60)+" minutes ---"

#print_quartiles()
