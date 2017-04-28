#!/usr/bin/env python
import scipy.optimize as o
import scipy.special as sp
from scipy.stats.stats import pearsonr 
from scipy.stats import  ks_2samp
from sklearn.metrics import r2_score
from scipy import stats
import time
import numpy as np
import corpus as c
import config as s
import model as m
import validation as v
import time
	
def topic_cite_model(domain,start_year,end_year):
	'''
	Fits and validates data to topic prediction model and stores result 
	'''
	#OUTPUT FILES
	directory = "./output/"+domain+"/"
	c.mkdir(directory)
	f1 = open(directory+"metrics_"+domain+".csv","w")
	f1.write("topic_id, KS_D, KS_p, mape, mae, rmse, nrmse, r2\n")
	f2 = open(directory+"predicted_"+domain+".csv","w")
	f2.write("topic_id, cite_year, y_test, pred_y\n")
	f3 = open(directory+"fit_params_"+domain+".csv","w")
	f3.write("topic_id,lambda,mu,sigma\n")
	
	#quartiles = c.quartile_calculation(domain,start_year,end_year) #threshold can be set to quartiles
	print "Loading ",domain," data"
	load_start_time = time.time()
	cite_dist = cuml_cite_dist(domain,start_year,end_year,threshold = 0)
	print "--- load time: "+str((time.time() - load_start_time)/60)+" minutes ---"
	
	print "Running topic predition model for",domain
	model_start_time = time.time()
	results = {"ks_d":[],"ks_p":[],"mape":[],"mae":[],"rmse":[],"nrmse":[],"r2":[]}
	
	for topic_id in cite_dist:
		start_year,x_train,y_train,x_test,y_test = _prepare_topic_data(cite_dist[topic_id])
		#fit the model usng non linear least squares
		lamb,mu,sigm,parm_cov = m.fitmodel(topic_id, x_train,y_train)
		#write fit parameters to file
		f3.write(topic_id+","+c.strnd(lamb)+","+c.strnd(mu)+","+c.strnd(sigm)+"\n")
		y_fit = m.model_func(x_test,lamb,mu,sigm)
		res = store_result(topic_id,f1,f2,start_year,x_test,y_fit,y_test)
		for metric in results : results[metric].append(res[metric])
	display_result(results)
	f1.close()
	f2.close()
	f3.close()
	print "--- model run time: "+str((time.time() - load_start_time)/60)+" minutes ---"


def cuml_cite_dist(domain,start_year,end_year,threshold=0.001):
	'''
	Returns cummulative citation distribution for each topic in domain. 
	dist = { topic_id: {yr1:cite_count_yr1, yr2:cite_count_yr2 ..}... }
	'''
	sql = c.sql_topic_cite_dist(domain,start_year, end_year,threshold)
	db,cursor = c.dbConnect()
	cursor.execute(sql)
	dist = {}
	rows = cursor.fetchall()
	
	for row in rows:
		topic_id = row[0]
		count = float(row[2])
		prob = float(row[3])
		year = int(row[1])
		if year <= end_year and year>=start_year:
			if topic_id in dist:
				dist[topic_id][year] = dist[topic_id][year] + (count*prob)
			else:
				dist[topic_id] = {yr: 0.0  for yr in xrange(start_year,end_year+1,1)}
				dist[topic_id][year] =(count * prob)
	dist = _make_cumulative(dist,end_year)
	return dist

def _make_cumulative(dist,end_year):
	'''
	removes years with zero citation, makes distribution cummulative
	'''
	for topic_id in dist:
		dist[topic_id] = {x:y for x,y in dist[topic_id].items() if y!=0.0}
		birth_year = min(dist[topic_id])
		for yr in xrange(birth_year+1,end_year+1,1):
			if yr in dist[topic_id]:
				dist[topic_id][yr] = dist[topic_id][yr] + dist[topic_id][yr-1]
			else:
				dist[topic_id][yr] = dist[topic_id][yr-1]
	return dist

def _prepare_topic_data(dist):
	'''
	splits citation data into test and train years for model fitting
	'''
	x_test,y_test,x_train,y_train = [],[],[],[]
	birth_year = min(dist)
	for yr in sorted(dist):
		if yr < s.train_limit:
			x_train.append(yr - birth_year + 1)
			y_train.append(dist[yr])
		else:
			x_test.append(yr - birth_year + 1)
			y_test.append(dist[yr])
	return birth_year,x_train,y_train,x_test,y_test 


def accuracy_metrics(id,predictions,targets):
	'''
	Calculates accuracy metrics for predictions: KS,MAPE,MAE,RMSE,NRMSE,R2
	'''
	#KS
	ks = ks_2samp( predictions,targets )
	#MAPE
	mape = v.mapef(predictions,targets)
	#MAE
	mae = v.maef(predictions,targets)
	#RMSE
	rmse = v.rmsef(predictions,targets)
	#NRMSE
	nrmse = -1
	maxy,miny = max(targets),min(targets)
	if maxy != miny:
		nrmse = rmse/ (maxy - miny)
	#r2
	r2 = r2_score(targets, predictions)
	return ks,mape,mae,rmse,nrmse,r2


def store_result(topic_id,f1,f2,start_year,x_test,y_pred,y_test):
	pred = np.array(y_pred)
	test = np.array(y_test)    
	ks,mape,mae,rmse,nrmse,r2 = accuracy_metrics("topic",pred,test)
	#write metrics to file
	f1.write(str(topic_id)+","+c.strnd(ks[0])+","+c.strnd(ks[1])+","+c.strnd(mape)+","+c.strnd(mae)+","+c.strnd(rmse)+","+c.strnd(nrmse)+","+c.strnd(r2)+"\n")
	#write predicted and empirical citations to file
	for i in xrange(len(pred)):
		f2.write(str(topic_id)+","+str(x_test[i]+start_year-1)+","+c.strnd(test[i])+","+c.strnd(pred[i])+"\n")
	res = {"ks_d":ks[0],"ks_p":ks[1],"mape":mape,"mae":mae,"rmse":rmse,"nrmse":nrmse,"r2":r2}
	return res

def display_result(results):
	for metric in results:
		print metric,"- avg:",c.strnd(np.mean(results[metric])),",median:",c.strnd(np.median(results[metric])),"min:",c.strnd(min(results[metric])),",max:",c.strnd(max(results[metric]))
	

