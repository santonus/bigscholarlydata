from gensim import corpora, models, similarities,matutils
from gensim.models.wrappers.dtmmodel import DtmModel

import mysql.connector
import numpy

def dtm_db(model, db, corpus, id_dict, time_ranges, num_topics=40):
	cur = db.cursor()
	cur.execute("CREATE TABLE if not exists TopicKeywords_DTM_se_"+str(num_topics)+"(topic_id varchar(255) NOT NULL,keyword varchar(255) NOT NULL, prob DOUBLE(3,3), year_begin int, year_end int, PRIMARY KEY (topic_id, keyword, year_begin, year_end))")
	cur.execute("CREATE TABLE if not exists DocTopic_DTM_se_"+str(num_topics)+"(paper_id varchar(255) NOT NULL,topic_id varchar(255) NOT NULL,prob DOUBLE(3,3), PRIMARY KEY (paper_id,topic_id))")
	cur.execute("delete from TopicKeywords_DTM_se_"+str(num_topics))
	cur.execute("delete from DocTopic_DTM_se_"+str(num_topics))

	i = 0
	j = 0
	topics_all = model.show_topics(num_topics=num_topics, times=len(time_ranges), num_words=15, log=False, formatted=True)
	#print "topics_all",topics_all
	print("Inserting Topic- Keyword DTM")
	for topic in topics_all:
		topic_num = str(i)
		year_begin, year_end = time_ranges[j]
		i = i + 1
		keyword = topic.split("+")
		for key in keyword:
			try:
				prob, kw = key.split('*')
				cur.execute("insert into TopicKeywords_DTM_se_"+str(num_topics)+" (topic_id,keyword,prob,year_begin,year_end) values (\"Topic_"+topic_num+"\",\""+kw+"\",\""+prob+"\",\""+str(year_begin)+"\",\""+str(year_end)+"\");")
			except:
				print "topic:",topic,"key",key
		if i == num_topics:
			i = 0
			j = j + 1
	#print "len(gamma_)",len(gamma_)
	#print "gamma_[0]",gamma_[0]		
	print("Inserting Doc Topic DTM")
	for i in range(0,len(corpus)):
		for j in range(0,num_topics):
			topic_num, prob = j, model.gamma_[i,j]
			paper_id = id_dict[i]
			cur.execute("insert into DocTopic_DTM_se_"+str(num_topics)+" (paper_id,topic_id,prob) values (\""+paper_id+"\",\""+"Topic_"+str(topic_num)+"\",\""+str(prob)+"\");")
	
	db.commit()


def blei_lda_db(model, db, corpus, id_dict, start_year,end_year,num_topics=80):
	
	t = "se_cuml_"+str(start_year)+"_"+str(end_year)
	cur = db.cursor()
	cur.execute("CREATE TABLE if not exists TopicKeywords_"+t+"_"+str(num_topics)+"(topic_id varchar(255) NOT NULL,keyword varchar(255) NOT NULL, prob DOUBLE(3,3),PRIMARY KEY (topic_id,keyword))")
	cur.execute("CREATE TABLE if not exists DocTopic_"+t+"_"+str(num_topics)+"(paper_id varchar(255) NOT NULL,topic_id varchar(255) NOT NULL,prob DOUBLE(3,3), PRIMARY KEY (paper_id,topic_id))")
	cur.execute("delete from DocTopic_"+t+"_"+str(num_topics))
	cur.execute("delete from TopicKeywords_"+t+"_"+str(num_topics))

	for topic in model.show_topics(num_topics=num_topics,num_words=15):
		topic_no =  str(topic[0])
		keyword = str(topic[1]).split('+')
		for key in keyword:
			try:
				prob, kw = key.split('*')
				cur.execute("insert into TopicKeywords_"+t+"_"+str(num_topics)+" (topic_id,keyword,prob) values (\"Topic_"+topic_no+"\",\""+kw+"\",\""+prob+"\");")
			except Exception:
				print("Exception",prob,kw)
				continue
	i = 0
	for doc in corpus:
		for topic in model[doc]:
			topic_num,prob = topic
			paper_id = id_dict[i]
			cur.execute("insert into DocTopic_"+t+"_"+str(num_topics) +" (paper_id,topic_id,prob) values (\""+paper_id+"\",\""+"Topic_"+str(topic_num)+"\",\""+str(prob)+"\");")
		i = i+1

	db.commit()
	
