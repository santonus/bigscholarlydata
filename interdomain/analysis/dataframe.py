import common.utilities as u
from scipy.stats import pearsonr
from scipy.stats import spearmanr
import common.db as db
import common.config as c
import network.metrics as n

import numpy as np
import pandas as pd
import time
import re


def collect_kws(domain):
	data = [[],[]]
	conn,cursor = db.dbConnect()
	for i in range(0,c.domain_topics[domain],1):
		sql = db.get_topic_kws(domain,i)
		cursor.execute(sql)
		rows = cursor.fetchall()
		#print rows
		data[0].append(rows[0][0])
		data[1].append(rows[0][1].replace(",","-"))
	cursor.close()
	conn.close()
	return data

def collect_hi_med(domain,threshold):
	data = [[],[]]
	conn,cursor = db.dbConnect()
	for i in range(0,c.domain_topics[domain],1):
		sql = db.get_author_hi(domain,threshold,i)
		cursor.execute(sql)
		rows = cursor.fetchall()
		data[0].append(rows[0][0])
		data[1].append(np.median(u.col(rows,1)))
	cursor.close()
	conn.close()
	return data

def collect_cosim_med(domain):
	data = [[],[],[]]
	for i in range(0,c.domain_topics[domain],1):
		sim_mat,labels = u.load_sim_mat()
		if domain!="all":
			inter = u.sim_mat_indx_range(domain,i,type="intra")
			intra = u.sim_mat_indx_range(domain,i,type="inter")
			data[0].append("Topic_"+str(i))
			data[1].append(np.median([sim_mat[u.sim_mat_indexer(domain,i)][j] for j in intra]))
			data[2].append(np.median([sim_mat[u.sim_mat_indexer(domain,i)][j] for j in inter]))
		else:
			intra = u.sim_mat_indx_range(domain,i,type="inter")
			data[0].append("Topic_"+str(i))
			data[1].append(np.median([sim_mat[u.sim_mat_indexer(domain,i)][j] for j in intra]))
		#data[3].append(u.sim_mat_indexer(domain,i))
	return data

def collect_rhl(domain):
	'''
	Extracts RHL values from csv as pandas DataFrame
	'''
	#rhl_pub = pd.read_csv("rhl/rhl_pub_"+domain+".csv").rename(columns={'rhl':'rhl.pub','topicid':'id','comment':'comment.pub'})
	#rhl_cite = pd.read_csv("rhl/rhl_cite_"+domain+".csv").rename(columns={'rhl':'rhl.cit','topicid':'id','comment':'comment.cit'})
	
	rhl_pub = pd.read_csv("rhl/rhl_pub_"+domain+".csv").rename(columns={'rhl':'rhl.pub','topicid':'id'})
	rhl_cite = pd.read_csv("rhl/rhl_cite_"+domain+".csv").rename(columns={'rhl':'rhl.cit','topicid':'id'})
	rhl_pub = rhl_pub[['id','rhl.pub']].copy()
	rhl_cite = rhl_cite[['id','rhl.cit']].copy()
	#rhl_pub = rhl_pub[['id','rhl.pub','comment.pub']].copy()
	#rhl_cite = rhl_cite[['id','rhl.cit','comment.cit']].copy()
	return pd.merge(rhl_pub,rhl_cite,how="inner")

def collect_net_intra(domain):
	sim_mat,labels = u.load_sim_mat()
	indx_range = [u.sim_mat_indexer(domain,0),u.sim_mat_indexer(domain,c.domain_topics[domain])]
	sim_mat = sim_mat[indx_range[0]:indx_range[1],indx_range[0]:indx_range[1]]
	labels = labels[indx_range[0]:indx_range[1]]
	node_labels = n.domain_labels(labels,domain=domain)
	threshold = n.cosim_quartile(sim_mat)
	g = n.generate_network(sim_mat,node_labels,threshold)
	df = n.network_measures(g)
	df = df.rename(columns={'degree':'degree.intra', 'clustcoeff':'clustcoeff.intra','betweenness':'betweenness.intra' , 'colseness':'colseness.intra' , 'eigenvector':'eigenvector.intra' , 'clustcoeff':'clustcoeff.intra' , 'pagerank':'pagerank.intra' })
	return df

def collect_network_inter():
	sim_mat,labels = u.load_sim_mat()
	#INTER
	node_labels = n.domain_labels(labels)
	threshold = n.cosim_quartile(sim_mat)
	g = n.generate_network(sim_mat,node_labels,threshold)
	df = n.network_measures(g)
	df = df.rename(columns={'degree':'degree.inter', 'clustcoeff':'clustcoeff.inter','betweenness':'betweenness.inter' , 'colseness':'colseness.inter' , 'eigenvector':'eigenvector.inter' , 'clustcoeff':'clustcoeff.inter' , 'pagerank':'pagerank.inter' })
	return df

def collect_data(d):
	conn,cursor = db.dbConnect()
	quartiles = u.quartile_calculation(d)
	threshold = quartiles[2]
	#topic_id,label,domain
	rows = db.dbExecute(cursor,db.get_topic_labels(d))
	df1 = pd.DataFrame({'id':u.col(rows,0),'label':u.col(rows,1),'domain':[d]*c.domain_topics[d]})
	#topic max min years
	rows = db.dbExecute(cursor,db.get_maxmin_years(d,threshold))
	df2 = pd.DataFrame({'id':u.col(rows,0),'max.yr':u.col(rows,1),'min.yr':u.col(rows,2)})
	df2['lifetime']=df2['max.yr']-df2['min.yr']
	df1 = pd.merge(df1,df2, how='inner')
	#print "2:",df1
	#topic citations
	rows = db.dbExecute(cursor,db.get_weighted_citations(d,threshold))
	df2 = pd.DataFrame({'id':u.col(rows,0),'w.cites':u.col(rows,1) })
	df1 = pd.merge(df1,df2, how='inner')
	#unweighted
	rows = db.dbExecute(cursor,db.get_unweighted_citations(d,threshold))
	df2 = pd.DataFrame({'id':u.col(rows,0),'nw.cites':u.col(rows,1) })
	df1 = pd.merge(df1,df2, how='inner')
	#topic kws
	rows = collect_kws(d)
	df2 = pd.DataFrame({'id':rows[0],'ten.kwd':rows[1]})
	df1 = pd.merge(df1,df2, how='inner')
	#topic papers
	rows = db.dbExecute(cursor,db.get_topic_papers(d,threshold))
	df2 = pd.DataFrame({'id':u.col(rows,0),'papers':u.col(rows,1) })
	df1 = pd.merge(df1,df2, how='inner')
	#topic venues
	rows = db.dbExecute(cursor,db.get_topic_venues(d,threshold))
	df2 = pd.DataFrame({'id':u.col(rows,0),'venues':u.col(rows,1) })
	df1 = pd.merge(df1,df2,how="inner")
	#topic authors
	rows = db.dbExecute(cursor,db.get_topic_authors(d,threshold))
	df2 = pd.DataFrame({'id':u.col(rows,0),'authors':u.col(rows,1) })
	df1 = pd.merge(df1,df2,how="inner")
	#topic authors median hi
	rows = collect_hi_med(d,threshold)
	df2 = pd.DataFrame({'id':rows[0],'med.hindex':rows[1]})
	df1 = pd.merge(df1,df2,how="inner")
	#cosim intra,inter
	rows = collect_cosim_med(d)
	if domain !="all":
		df2 = pd.DataFrame({'id':rows[0],'med.cosim.intra':rows[1] ,'med.cosim.inter':rows[2]}) # WITH INTER
	else:
		df2 = pd.DataFrame({'id':rows[0],'med.cosim.intra':rows[1] })#,'cosim.mat.indx':rows[3]}) # ALL TOPICS
	df1 = pd.merge(df1,df2,how="inner")
	#RHL cit,pub
	#df2 = collect_rhl(d)
	#df1 = pd.merge(df1,df2,how="inner")
	#df2 = collect_net_intra(d)
	#df1 = pd.merge(df1,df2,how="left",on=['domain','label'])
	#Domain coordinates
	df1["d.x"] = df1.apply(lambda row: c.domain_cord[row.domain][0], axis=1)
	df1["d.y"] = df1.apply(lambda row: c.domain_cord[row.domain][1], axis=1)
	df1["d.z"] = df1.apply(lambda row: c.domain_cord[row.domain][2], axis=1)
	return df1

def create_domain_df():
	'''
	function to create data frame
	'''
	start_time = time.time()
	print "Creating dataframe"
	stat_data = pd.DataFrame()
	for d in c.domains:
		df = collect_data(d)
		print d,df.shape
		df.to_csv('stats_data/'+d+'_data.csv',sep=',',index=False)
		#if not stat_data: stat_data=pd.DataFrame(columns=df.columns)
		stat_data = pd.concat([stat_data,df],ignore_index=True)  

	#print "all",stat_data.shape
	df = collect_network_inter()
	stat_data = pd.merge(stat_data,df,how="inner")
	u.mkdir("./stats_data")
	stat_data.to_csv('stats_data/all_data.csv',sep=',',index=False)
	print "--- time for dataframe generation "+str((time.time() - start_time)/60)+" minutes ---"	

def correlation_mat(df,fname,path,save="pair"):
	num_cols = df.select_dtypes(include=[np.number]).columns
	spr_mat = spearmanr(df.as_matrix(columns=num_cols))
	prs_mat = u.corr_pearson(df[num_cols])
	#writing to file
	if save=="pair":
		f1 = open(path+fname+"_spearman"+".csv","w")
		f2 = open(path+fname+"_pearson"+".csv","w")
		f1.write("col1,col2,corr,pval\n")
		f2.write("col1,col2,corr,pval\n")
		for (i,j) in  zip(np.triu_indices(len(num_cols),k=1)[0],np.triu_indices(len(num_cols),k=1)[1]):	
				#print "--",i,j
				f1.write(num_cols[i]+","+num_cols[j]+","+str(round(spr_mat[0][i][j],3))+","+str(round(spr_mat[1][i][j],3))+"\n")
				f2.write(num_cols[i]+","+num_cols[j]+","+str(round(prs_mat[0][i][j],3))+","+str(round(prs_mat[1][i][j],3))+"\n")
				
	if save == "mat":		
		pd.DataFrame(data=spr_mat[0],index=num_cols,columns=num_cols).to_csv(path+"spearmanR_"+fname+".csv")
		pd.DataFrame(data=spr_mat[1],index=num_cols,columns=num_cols).to_csv(path+"p-value_spearmanR_"+fname+".csv")
		pd.DataFrame(data=prs_mat[0],index=num_cols,columns=num_cols).to_csv(path+"pearsonR_"+fname+".csv")
		pd.DataFrame(data=prs_mat[1],index=num_cols,columns=num_cols).to_csv(path+"p-value_pearsonR_"+fname+".csv")
	
def desc_stat_data(**kwargs):
	for fpath in kwargs.get("files"):
		#path to where the files will be saved
		path = u.mkdir("./stats_data/desc/")
		df = pd.read_csv(fpath)
		r = re.compile('/(.*?).csv')
		fname = r.search(fpath).group(1).split("-")[0]
		#correlation between columns of stat_all
		'''
		correlation_mat(df,fname,path)
		
		#description of columns
		desc_df = df.describe()
		
		k = df.kurtosis().to_frame("kurtosis").transpose()
		skew = df.skew().to_frame("skew").transpose()
		desc_df = pd.concat([k,skew,desc_df])
		
		desc_df.round(4).to_csv(path+"desc_"+fname+".csv",sep=',')
		for col in desc_df.columns:
			gname = path+fname+"/"+col+"_distplot_"+fname+".png"
			u.dist_plot(df[col],gname)
			gname = path+fname+"/"+col+"_boxplot_"+fname+".png"
			u.box_plot(df[col],gname)
		'''