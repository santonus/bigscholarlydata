#This script creates paper-topic matrix with paper as rows and topic as columns and the matrix entry being the LDA probability with which the paper touches upon that topic
import numpy as np
import mysql.connector
import db_constants as dbc
#set global paramters
USER="bigdataproj"
PWD="bigdataproj@123"
HOST="n12",
DB="data_AM"

num_topics = 60
end_year = 2013
start_year = 1970
output_dir = ""

def generate_matrix(domain,start_year,end_year_range):				
	sql = "select d.topic_id, d.paper_id,d.prob from DocTopic_se_"+str(num_topics)+" d join pub_se p on p.paper_id = d.paper_id where p.pub_year < "+str(end_year)+" and p.pub_year >= "+str(start_year)+" order by d.paper_id"
	db,cursor = dbc.dbConnect()
	cursor.execute(sql)
	row = cursor.fetchone()
	# mat is {paperid : topics list}
	mat = {}
	while row is not None:
		paper = row[1]
		prob = float(row[2])
		topic = int(row[0].replace("Topic_",""))
		if paper in mat:
			mat[paper][topic] = prob
		else:
			mat[paper] = [ 0 for i in xrange(0,80,1)]
			mat[paper][topic] = prob
		row = cursor.fetchone()
	j = 1 #index
	f = open(output_dir+"Paper_Topic_weighted_mat_"+domain+"_"+str(num_topics)+".csv","w")
	# heading of csv file.
	h="paper,"  
	for i in xrange(0,num_topics): #
		if i!=0:
			h=h+",Topic_"+str(i)
		else:
			h=h+"Topic_"+str(i)
	h = h + "\n"
	f.write(h)
	# writing matrix to file
	for paper in mat:
		if paper!="":
			s = "" #entry row
			if sum(mat[paper] )> 1: #for papers with sum of prob > 1, To deal with this bug we have currently removed the 0.002 topics from the papers which have sum(prob) > 1.
				for i in xrange(0,80,1):
					if mat[paper][i]<=0.002:
						s = s + ","+str(0.0)
					else:
						s = s + ","+str(mat[paper][i])
			else:
				for i in xrange(0,80,1):
					s = s + ","+str(mat[paper][i])
			f.write(paper+s+"\n")
	db.close()

#generate matrix for all domains
for domain in ["se","ai","os","db"]:
 	generate_matrix(domain,start_year,end_year)