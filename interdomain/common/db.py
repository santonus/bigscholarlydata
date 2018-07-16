import common.config as c
import mysql.connector

def dbConnect():
	cnx = mysql.connector.connect(user=c.DBUSER, password=c.DBPWD,host =c.DBHOST,database=c.DB)
	cursor = cnx.cursor()
	cursor.execute('set global max_allowed_packet=67108864')
	return cnx,cursor

def dbExecute(cursor,sql):
	cursor.execute(sql)
	rows = cursor.fetchall()
	return rows

def get_topic_domain_dist(d,threshold):
	sql = "select t.topic_id,d.domain,count(d.domain) from PaperDomain d join DocTopic_3gram_"+d+"_"+str(c.domain_topics[d])+" t on t.paper_id = d.paper_id where prob >= "+str(threshold)+" group by t.topic_id, d.domain order by t.topic_id , d.domain"
	return sql

def get_topic_l2(domain):
	sql = "SELECT topic_id,SUM(POWER(prob,2)) as mod2 FROM TopicKeywords_3gram_"+domain+"_"+str(c.domain_topics[domain])+" GROUP BY topic_id"
	return sql

def get_topic_cross(d1,d2):
	#sql = "SELECT  a.topic_id t1, b.topic_id t2, a.prob * b.prob as mul FROM (SELECT * from TopicKeywords_3gram_"+d1+"_"+str(c.domain_topics[d1])+" where prob >=0.000001) a JOIN (select *  from TopicKeywords_3gram_"+d2+"_"+str(c.domain_topics[d2])+" where prob >=0.000001) b ON a.keyword=b.keyword"
	sql = "SELECT  a.topic_id t1, b.topic_id t2, a.prob * b.prob as mul FROM (SELECT * from TopicKeywords_3gram_"+d1+"_"+str(c.domain_topics[d1])+") a JOIN (select *  from TopicKeywords_3gram_"+d2+"_"+str(c.domain_topics[d2])+") b ON a.keyword=b.keyword"
	return sql
def keyword_dot(d1,t1,d2,t2):
	sql = "Select sum(mul) as dot from (SELECT a.prob * b.prob as mul FROM (SELECT * from TopicKeywords_3gram_"+d1+"_"+str(c.domain_topics[d1])+" WHERE topic_id='"+t1+"' ) a JOIN (select *  from TopicKeywords_3gram_"+d2+"_"+str(c.domain_topics[d2])+" WHERE topic_id='"+t2+"' ) b ON a.keyword=b.keyword) as c"
	return sql

def get_topic_labels(domain):
	sql = "SELECT topic_id, label FROM TopicLabels_3gram_"+domain+"_"+str(c.domain_topics[domain])
	return sql

def get_maxmin_years(domain,threshold):
	sql = "SELECT d.topic_id,max(p.pub_year),min(p.pub_year) FROM DocTopic_3gram_"+domain+"_"+str(c.domain_topics[domain])+" d join pub_"+domain+" p on p.paper_id=d.paper_id where p.pub_year <= 2013 AND d.prob >="+str(threshold)+"group by topic_id"
	return sql

def get_topic_paper_probs(domain,threshold=0):
	#sql = "select max(t.prob) from (select d.prob as prob , d.paper_id as paper_id from DocTopic_3gram_"+domain+"_"+str(domain_topics[domain])+" d join pub_"+domain+" p on p.paper_id=d.paper_id where p.pub_year >=1975 and p.pub_year < 2013) t group by t.paper_id"
	if domain=="all":
		sql = "select d.prob as prob from DocTopic_3gram_"+domain+"_"+str(c.domain_topics[domain])+" d where prob >="+str(threshold)+" limit 10"
	else:
		sql = "select d.prob as prob from DocTopic_3gram_"+domain+"_"+str(c.domain_topics[domain])+" d join pub_"+domain+" p on p.paper_id=d.paper_id where p.pub_year <= 2013"
	return sql

def get_unweighted_citations(domain,threshold):
	sql = "SELECT d.topic_id,count(c.cites_id) FROM DocTopic_3gram_"+domain+"_"+str(c.domain_topics[domain])+" d join cite_"+domain+" c on c.id=d.paper_id where c.pub_year <= 2013 AND d.prob >="+str(threshold)+"group by topic_id"
	return sql

def get_weighted_citations(domain,threshold):
	cites = "SELECT id,count(cites_id) as cites,pub_year FROM cite_"+domain+" group by id"
	sql = "SELECT d.topic_id,sum(d.prob*c.cites) FROM DocTopic_3gram_"+domain+"_"+str(c.domain_topics[domain])+" d join ("+cites+") c on c.id=d.paper_id where c.pub_year <= 2013 AND d.prob >="+str(threshold)+"group by topic_id"
	return sql

def get_topic_papers(domain,threshold):
	sql = "select topic_id,count(distinct paper_id) from DocTopic_3gram_"+domain+"_"+str(c.domain_topics[domain])+" where prob >="+str(threshold)+"group by topic_id"
	return sql

def get_topic_venues(domain,threshold):
	sql = "select d.topic_id,count(distinct p.venue_id) from DocTopic_3gram_"+domain+"_"+str(c.domain_topics[domain])+" d join pub_"+domain+" p on p.paper_id = d.paper_id"+" where d.prob >="+str(threshold)+" group by d.topic_id"
	return sql

def get_doctopic_se():
	'''
	returns doctopic of se with AM paper id instead of MAG paper ids
	'''
	return "select d.topic_id as topic_id,m.a_id as paper_id,d.prob as prob from MAG_AM_mapping m join DocTopic_3gram_se_40 d on m.m_id=d.paper_id"

def get_topic_authors(domain,threshold):
	if domain=="se":
		sql = sql = "select d.topic_id,count(distinct a.author_id) from paper_author_all a join ("+ get_doctopic_se()+") d on d.paper_id = a.paper_id where d.prob >="+str(threshold)+"group by topic_id"
	else:
		sql = "select d.topic_id,count(distinct a.author_id) from paper_author_"+domain+" a join DocTopic_3gram_"+domain+"_"+str(c.domain_topics[domain])+" d on d.paper_id = a.paper_id where d.prob >="+str(threshold)+"group by topic_id"
	return sql


def get_topic_kws(domain,topic_id):
	sql ="select topic_id, GROUP_CONCAT(keyword) from ( select * from TopicKeywords_3gram_"+domain+"_"+str(c.domain_topics[domain])+" where topic_id='Topic_"+str(topic_id)+"' order by prob desc limit 10) a"
	return sql

def get_author_hi(domain,threshold,topic_id):
	if domain =="se":
		authors = "select d.topic_id as topic_id,p.author_id as author_id from ("+get_doctopic_se()+") d join paper_author_"+domain+" p on p.paper_id=d.paper_id where topic_id='Topic_"+str(topic_id)+"' and prob >="+str(threshold)
		sql ="select a.topic_id, hi from ("+authors+") a join author_"+domain+" at on at.author_id=a.author_id"
	else:	
		authors = "select d.topic_id as topic_id,p.author_id as author_id from DocTopic_3gram_"+domain+"_"+str(c.domain_topics[domain])+" d join paper_author_"+domain+" p on p.paper_id=d.paper_id where topic_id='Topic_"+str(topic_id)+"' and prob >="+str(threshold)
		sql ="select a.topic_id, hi from ("+authors+") a join author_"+domain+" at on at.author_id=a.author_id"

	return sql
