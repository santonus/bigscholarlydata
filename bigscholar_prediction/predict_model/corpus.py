import mysql.connector
import config as s
import numpy as np
import pandas as pd
import os


all_domains = ["se","ai","os","db"]

def strnd(num):
	return str(round(num,2))
	
def pubyr_range(domain):
	'''
	Returns min, max year of publications of papers of domain
	'''
	db,cursor = dbConnect()
	sql = "select min(pub_year),max(pub_year) from pub_"+domain
	cursor.execute(sql)	
	rows = cursor.fetchall()
	return rows[0][0],rows[0][1]

def dbConnect():
	cnx = mysql.connector.connect(user=s.DBUSER, password=s.DBPWD,host = s.DBHOST,database=s.DB)
	cursor = cnx.cursor()
	return cnx,cursor

def sql_topic_cite_dist(domain,start_year,end_year, threshold=0.001):
	'''
	sql to return topic_id, cite_year,cite_count,prob for each paper published in [start_year,end_year] and touching on topic_id
	'''
	cite_count = "SELECT id as paper_id,cite_year,count(distinct cites_id) as cite_count from cite_"+domain+" WHERE pub_year>="+str(start_year)+" AND pub_year<="+str(end_year)+" group by id,cite_year"
	sql = "SELECT t.topic_id, c.cite_year,c.cite_count,t.prob from DocTopic_"+domain+"_"+str(s.num_topics)+" t JOIN ("+cite_count+") c ON c.paper_id=t.paper_id WHERE t.prob > "+str(threshold)+" ORDER BY t.topic_id,c.cite_year"
	return sql

def sql_topic_pub_dist(domain,start_year,end_year, threshold=0.001):
	sql = "SELECT t.topic_id,p.pub_year,count(distinct t.paper_id) from DocTopic_"+domain+"_"+str(s.num_topics)+" t join pub_"+domain+" p on t.paper_id = p.paper_id WHERE t.prob > "+str(threshold)+ " GROUP BY t.topic_id,p.pub_year order by t.topic_id,p.pub_year"
	return sql

def mkdir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)

def quartile_calculation(domain,start_year,end_year):
	'''
	Calculates quartiles from probability distributions of paper topic relation for.
	Return: array of the three quartile values
	'''
	db,cursor = dbConnect()
	sql = "select max(t.prob) from (select d.prob as prob , d.paper_id as paper_id from DocTopic_"+domain+"_"+str(s.num_topics)+" d join pub_"+domain+" p on p.paper_id=d.paper_id where p.pub_year >="+ str(start_year)+" and p.pub_year < "+str(end_year)+") t group by t.paper_id"
	cursor.execute(sql)
	rows = cursor.fetchall()
	db.close()
	quartiles = np.percentile(rows, np.arange(25, 100, 25)) # quartiles
	return quartiles