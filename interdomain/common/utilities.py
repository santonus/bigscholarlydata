import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
from scipy.stats import spearmanr
import numpy as np
import seaborn as sns
import common.config as c
import common.db as db
import csv
import os
'''
os quartiles: [ 0.001  0.002  0.003]
se quartiles: [ 0.015  0.041  0.11 ]
db quartiles: [ 0.001  0.002  0.003]
ai quartiles: [ 0.001  0.002  0.045]

'''
def gini(x):
	# (Warning: This is a concise implementation, but it is O(n**2)
	# in time and memory, where n = len(x).  *Don't* pass in huge
	# samples!)
	# Mean absolute difference
	mad = np.abs(np.subtract.outer(x, x)).mean()
	# Relative mean absolute difference
	rmad = mad/np.mean(x)
	# Gini coefficient
	g = 0.5 * rmad
	return g

def n_digit_list(data,n=3):
	data = [int(i.replace("Topic_","")) for i in data]
	return ["Topic_"+str(item).zfill(n) for item in data]

def unique_counts(x):
	'''
	Returns counts of unique values of a numpy array
	'''
	unique, counts = np.unique(x, return_counts=True)
	return np.asarray((unique, counts)).T

def box_plot(a,fname):
	ax = sns.boxplot(a)
	fig = ax.get_figure()
	fig.savefig(fname)
	plt.cla()   # Clear axis
	plt.clf()

def dist_plot(a,fname):
	ax = sns.distplot(a,norm_hist=False)
	fig = ax.get_figure()
	plt.ylabel('Density', fontsize=16)
	plt.xlabel('Values', fontsize=18)
	fig.savefig(fname)
	plt.cla()   # Clear axis
	plt.clf()

def hist_plot(a,fname,xlabel='',ylabel='',xticks=0):
	ax = sns.distplot(a,kde=False, rug=False,color="red")
	fig = ax.get_figure()
	plt.ylabel(ylabel, fontsize=18)
	plt.xlabel(xlabel, fontsize=18)
	ax.tick_params(labelsize=13)
	if xticks!=0:
		plt.xticks(np.arange(min(a), max(a)+xticks, xticks))
	fig.savefig(fname)
	plt.cla()   # Clear axis
	plt.clf()

def corr_pearson(df):
	corr = np.zeros((len(df.columns),len(df.columns)))	
	pval = np.zeros((len(df.columns),len(df.columns)))
	for i,d1 in enumerate(df.columns):
		for j,d2 in enumerate(df.columns):
			corr[i][j],pval[i][j] = pearsonr(df[d1],df[d2])
	return corr,pval

def mkdir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)
	return directory
def file_exists(fname):
	if os.path.isfile(fname):
		return True
	return False

def load_sim_mat(fname="all_sim_mat",rounded=True):
	data = np.load(fname)
	if rounded: 
		sim_mat = np.round(data['sim_mat'],decimals=4)
	else: sim_mat = data['sim_mat']
	labels=data['labels']

	return sim_mat,labels

def percentile(array,p=25):
	return np.percentile(array,np.arange(p,100,p))

#depr function
# def quartiles(d):
# 	return np.percentile(d,np.arange(25,100,25))

def paper_topic_percentile(d,p=25,threshold=0):
	'''
	Calculates quartiles from probability distributions of paper topic relation for.
	Return: array of the percentile values
	'''
	conn,cursor = db.dbConnect()
	sql = db.get_topic_paper_probs(d,threshold)
	print sql
	cursor.execute(sql)
	rows = cursor.fetchall()
	#print rows
	percentiles = percentile(rows,p=p) # percentiles
	print d,"percentiles:",percentiles
	conn.close()
	return percentiles	


#TODO: depr function, replace everywhere with percentile calculation
def quartile_calculation(d,threshold=0):
	'''
	Calculates quartiles from probability distributions of paper topic relation for.
	Return: array of the three quartile values
	'''
	conn,cursor = db.dbConnect()
	sql = db.get_topic_paper_probs(d,threshold)
	print sql
	cursor.execute(sql)
	rows = cursor.fetchall()
	#print rows
	quartiles = percentile(rows, 25) # quartiles
	print d,"quartiles:",quartiles
	conn.close()
	return quartiles	

def col(a,i):
	return [row[i] for row in a]

def extract_file(fname,delim=','):
	if fname[-4:]==".npy":
		data = np.load(fname)
		return data
	if fname[-4:]==".csv":
		df = pd.read_csv(fname, sep=delim, header=0)
		return df

def sim_mat_indexer(domain,topic_id):
	'''
	returns the index of the topic in the sim_mat of c.domains vs c.domains, given domain, topic pair
	'''
	n = 0
	for d in c.domains:
		if d == domain:
			break
		n = n + c.domain_topics[d]
	if isinstance(topic_id ,int):
		return n + topic_id		
	if isinstance(topic_id,str):
		return n + int(topic_id.replace("Topic_",""))
def sim_mat_i_indexer(index):
	'''
	returns domain and topic_id given sim mat sim_mat_indexer
	'''
	l = 0
	r = 0
	for d in c.domains:
		r = r + c.domain_topics[d]
		if index>=l and index<r:
			return d,"Topic_"+str(l+ index)
		else:
			l = r


def fmt_tid(s):
	return int(s.replace("Topic_",""))

def sim_mat_indx_range(domain,topic_id,type="intra"):
	'''

	'''
	if type=="intra":		
		ind = [sim_mat_indexer(domain,i) for i in range(0,c.domain_topics[domain]) if i!=topic_id]
		return ind
	if type=="inter":
		'''
			if d!=domain:
				ind = ind + sim_mat_indx_range(d,-1)
		'''
		ind = [i for i in range(0,c.domain_topics["all"])]
		ind.remove(sim_mat_indexer(domain,topic_id))	
		return ind
	
	
		