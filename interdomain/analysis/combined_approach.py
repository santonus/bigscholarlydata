import common.utilities as u
import common.db
import common.config as c

import numpy as np
import pandas as pd
import time
import re
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import precision_score
from sklearn.metrics import classification_report 
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

def column_analysis(**kwargs):
	fpath = kwargs.get("file")
	path = u.mkdir("./stats_data/desc/")
	df = pd.read_csv(fpath)
	for domain in c.domains:
		df2 = df[df.domain == domain]
		u.box_plot(df2["med.cosim.inter"],path+"box_med.cosim.inter_"+domain+".png")
		u.dist_plot(df2["med.cosim.inter"],path+"dist_med.cosim.inter_"+domain+".png")

def sim_mat_dist(**kwargs):
	'''
	Saves the sim mat in a file along with median in last column.
	Plots the hist and box plot of median cosim of each topic
	'''
	data = np.load(kwargs.get("file","sim_mat"))
	labels=data['labels']
	sim_mat = data['sim_mat']
	sim_mat = np.round(sim_mat,decimals=6)
	medians = np.median(sim_mat, axis=1)
	maxs = []
	sim_mat[sim_mat>1] = 1
	sim_mat_sorted = np.copy(sim_mat)
	#find second highest value as max will always be 1
	for row in sim_mat_sorted:
		row.sort()
		maxs.append(row[-2])
	
	print "saving sim_mat in stats_data/sim_mat/"+c.query_name+".csv"
	u.mkdir("stats_data/sim_mat/")
	f= open("stats_data/sim_mat/"+c.query_name+"_sim_mat.csv","w")
	f.write(","+",".join(labels)+",median"+","+"max"+"\n")
	for i,med in enumerate(medians):
		f.write(labels[i]+","+",".join(map("{:.6f}".format,sim_mat[i]))+","+str(medians[i])+","+str(maxs[i])+"\n")

	u.box_plot(maxs,"stats_data/sim_mat/box_plot_"+c.query_name+"_maxcosim.png")
	u.hist_plot(maxs,"stats_data/sim_mat/hist_plot_"+c.query_name+"_maxcosim.png",xlabel='Maximum cosine similarity',ylabel='Number of topics',xticks=0)

	u.box_plot(medians,"stats_data/sim_mat/box_plot_"+c.query_name+"_median.png")
	u.hist_plot(medians,"stats_data/sim_mat/hist_plot_"+c.query_name+"_median.png",xlabel='median co-sim',ylabel='Number of topics',xticks=0)

def common_keywords_analysis(**kwargs):
	'''
	Find the top 20 topic pairs with the highest cosine similarity and reports the common keywords
	'''
	data = np.load(kwargs.get("file","sim_mat"))
	labels=data['labels']
	sim_mat = data['sim_mat']
	m = np.copy(sim_mat)
	#make lower triangle zero
	for i in range(0,c.domain_topics["all"]):
		for j in range(0,c.domain_topics["all"]):
			if i>=j:
				m[i][j]=0
	print np.argsort(m)[-2:]

def reg_analysis(**kwargs):
	for fpath in kwargs.get("files"):
			#path to where the files will be saved
			path = u.mkdir("./stats_data/desc/")
			df = pd.read_csv(fpath)
			r = re.compile('/(.*?).csv')
			fname = r.search(fpath).group(1).split("-")[0]
			fname = fname[:3]+"."+fname[3:]
			cols = [col for col in  df.columns if col!=fname and col!='id']
			print "Running Regression Analysis for",fname
			y = np.array(df[fname])
			X = np.array(df[cols])
			#print "X",X.shape,"y",y.shape
			X_train, X_test, y_train, y_test =train_test_split(X, y, test_size=0.33,random_state=42)
			#print "X_train",X_train.shape,"y_train",y_train.shape
			regr = RandomForestRegressor(max_depth=2, random_state=0)
			regr.fit(X_train, y_train)
			y_pred = regr.predict(X_test)
			zipped = zip(regr.feature_importances_,df[cols].columns)
			zipped.sort(key = lambda t: t[0])
			for imp,f in zipped:
				print f,":",imp
			print "-------- R2 score",regr.score(X_test,y_test),"----------"

def class_analysis(**kwargs):
	fpath = kwargs.get("file")
	rhl = ["rhl.cit","rhl.pub"]
	
	for fname in rhl:
		print "Classification Analysis for",fname
		df = pd.read_csv(fpath)
		quartiles = u.quartiles([i for i in df[fname] if i!=-1])
		if fname=="rhl.cit":
			
			df = df[df["rhl.cit" ] > quartiles[0] ]
			#df2 = df[ df["rhl.cit" ] < 0]
			#df = pd.concat([df1,df2])
			
			df = df.drop("rhl.pub",axis=1)
			df = df.drop("comment.pub",axis=1)
		else:
			
			df = df[df["rhl.pub" ] > quartiles[0] ]
			#df2 = df[ df["rhl.pub" ] < 0]
			#df = pd.concat([df1,df2])
			
			df = df.drop("rhl.cit",axis=1)
			df = df.drop("comment.cit",axis=1)
		comment = []
		print "Quartiles",quartiles
		for i in df[fname]:
			if i ==-1:
				#comment.append("G")
				pass
			elif i >=1 and i<=quartiles[0]:
				comment.append("D4Q")
				#pass
			elif i>=quartiles[0] and i<quartiles[1]:
				comment.append("D3Q")
			elif i>=quartiles[1] and i<quartiles[2]:
				comment.append("D2Q")
			elif i>=quartiles[2]:
				comment.append("D1Q")
			else:
				print i

		df["comment"+fname[3:]] = comment
		cols = [col for col in  df.columns if col!=fname and col!='id' and "comment"+fname[3:]]		
		

		#print df[cols].select_dtypes(include=np.number).columns
		cols = df[cols].select_dtypes(include=np.number).columns
		y = np.array(df["comment."+fname[4:]])
		X = np.array(df[cols])

		print "X",X.shape,"y",y.shape
		X_train, X_test, y_train, y_test =train_test_split(X, y, test_size=0.33,random_state=42,stratify =y)
		
		print "Label counts of train set:",u.unique_counts(y_train)
		print "Label counts of test set",u.unique_counts(y_test)
		
		clf = RandomForestClassifier(max_depth=2, random_state=42)
		#clf = GaussianNB()
		clf.fit(X_train, y_train)
		y_pred = clf.predict(X_test)
		
		#------Feature Importance----------

		zipped = zip(clf.feature_importances_,cols)
		zipped.sort(key = lambda t: t[0])
		for imp,f in zipped:
			print f,":",imp
		
		print classification_report(y_test, y_pred,labels=np.unique(y))
		print "-------- mean accuracy score",clf.score(X_test,y_test),"----------"

def DocTopic_dist():
	'''
	This function generates a box plot and other descriptive metrics of topic-paper prob distribution for the entire coupus
	'''
	conn,cursor = db.dbConnect()
	sql = db.get_topic_paper_probs("all")
	cursor.execute(sql)
	rows = cursor.fetchall()
	percentiles = u.percentile(rows,p=10)
	u.dist_plot(rows,"stats_data/desc/DocTopic_"+c.query_name+"_distplot.png")

def topic_paper_analysis():
	'''
	Analyze topic paper distribution in partitioned approach
	'''
	conn,cursor = db.dbConnect() 
	threshold = 0.001
	tcount = []
	for d in ["se","os","ai","db"]:
		sql = db.get_topic_domain_dist(d,threshold)
		cursor.execute(sql)
		t1 = time.time()
		rows = cursor.fetchall()
		dcount = []
		for row in rows:
			dcount.append(int(row[2]))
			tcount.append(int(row[2]))
		print "Gini for",d,"is",u.gini(dcount)

	print "Gini for journal:",u.gini(tcount)

def topic_domain_analysis():
	'''
	Analyze topic-domain distributions of papers in soup approach
	'''
	conn,cursor = db.dbConnect()
	quartiles = u.paper_topic_percentile("all",p=10)
	threshold = quartiles[c.threshold_quartile]
	
	#threshold = 0.001

	#print str(c.threshold_quartile+1),"th threshold is",threshold
	
	sql = db.get_topic_domain_dist("all",threshold)
	#print "sql",sql
	cursor.execute(sql)
	t1 = time.time()
	rows = cursor.fetchall()
	topic_lists = {"se":[],"db":[],"ai":[],"os":[]}
	topics = []
	# generate domain wise columns 
	current = {"se":0,"db":0,"ai":0,"os":0}
	for row in rows:
		if not topics:
			topics.append(row[0])
		if topics[-1]!=row[0] and topics:
			for d in current: 
				topic_lists[d].append(current[d])
			topics.append(row[0])
			current = {"se":0,"db":0,"ai":0,"os":0}
		current[row[1]]=int(row[2])
	for d in current: topic_lists[d].append(current[d]) #last topic

	#Convert int  to 3 digit int
	topics = u.n_digit_list(topics)
	#Make dataframe
	df = pd.DataFrame({'topic_id':topics,'se':topic_lists["se"],'os':topic_lists["os"],'db':topic_lists["db"],'ai':topic_lists["ai"]})
	
	df["total"] = df.sum(axis=1)
	gini = {}
	for d in ["se","ai","db","os"]:
		gini[d]=u.gini(topic_lists[d])
	gini["total"] = u.gini(df["total"].tolist())

	print gini
	#Generate dist/box plots for each domain
	for col in df.select_dtypes(include=[np.number]).columns:
		data = df[col].tolist()
		u.box_plot(data,"stats_data/desc/"+col+"_soup_boxplot.png")
		u.dist_plot(data,"stats_data/desc/"+col+"_soup_distplot.png")

	#Save distribution for each topic in csv
	df = df[['topic_id','se','ai','db','os','total']]
	print "saving domain topic analysis results in ",'stats_data/TopicDomain_'+c.query_name+'_Q'+str(c.threshold_quartile)+'.csv'
	df.to_csv('stats_data/TopicDomain_'+c.query_name+'_Q'+str(c.threshold_quartile)+'.csv',sep=',',index=False)
	f = open('stats_data/TopicDomain_'+c.query_name+'_Q'+str(c.threshold_quartile)+'.csv', "a")
	f.write("\ngini,"+gini["se"]+","+gini["ai"]+","+gini["db"]+","+gini["os"]+","+gini["total"]+"\n")
