import numpy as np
import networkx as nx
import common.config as c
import common.utilities as u
import pandas as pd
import matplotlib.pyplot as plt
import time

def domain_labels(labels,domain="None"):
	domain_index=[]
	if len(labels)==c.domain_topics["all"]:
		for d in c.domains:
			domain_index = domain_index+[d]*c.domain_topics[d]
	else:
		domain_index = [domain] * c.domain_topics[domain]

	node_labels=[domain_index[i]+"_"+label for i,label in enumerate(labels)]
	return node_labels

def generate_network(sim_mat,node_labels,threshold,weighted=False,**kwargs):
	g = nx.Graph()
	#print node_labels
	#print np.triu_indices(c.domain_topics["all"],k=1)
	#print len(np.triu_indices(c.domain_topics["all"],k=1)[0]),len(np.triu_indices(c.domain_topics["all"],k=1)[1])
	# Only considering upper triangle matrix
	g.add_nodes_from(node_labels)
	n = sim_mat.shape[0]
	for (x,y) in  zip(np.triu_indices(n,k=1)[0],np.triu_indices(n,k=1)[1]):
		if sim_mat[x][y]>= threshold:
			# if g.has_edge(node_labels[x],node_labels[y]):
			#print "has edge",node_labels[x],node_labels[y]
			if weighted:
				g.add_edge(node_labels[x],node_labels[y],weight=sim_mat[x][y])
			else:
				g.add_edge(node_labels[x],node_labels[y])
	print "nodes:",g.number_of_nodes(),"edges",g.number_of_edges()
	if kwargs.get("save",False):
		to_pajek(g,kwargs.get("gname","network"))
	return g

def network_measures(g):
	node_labels = g.nodes()
	# trans = nx.transitivity(g)
	# print "transitivity",trans

	clus_coeff = nx.clustering(g)
	#print "avg clustering coefficients",np.mean(clus_coeff.values())

	'''
	The degree centrality for a node v is the fraction of nodes it is connected to.
	'''
	degree_cent = nx.degree_centrality(g)
	#print "avg degree_centrality",np.mean(degree_cent.values())
	
	'''
	Closeness centrality of a node u is the reciprocal of the sum of the shortest path distances from u to all n-1 other nodes. Higher values of closeness indicate higher centrality.
	'''
	closeness_cent = nx.closeness_centrality(g)
	#print "avg closeness_centrality",np.mean(degree_cent.values())
	
	'''
	Betweenness centrality of a node v is the sum of the fraction of all-pairs shortest paths that pass through v:
	'''
	betw_cent = nx.betweenness_centrality((g))
	#print "avg betweenness_centrality",np.mean(betw_cent.values())

	'''
	Betweenness centrality of a node v is the sum of the fraction of all-pairs shortest paths that pass through v:
	'''
	eigenv_cent = nx.eigenvector_centrality(g)
	#print "avg eigenvector_centrality",np.mean(eigenv_cent.values())
	'''
	PageRank computes a ranking of the nodes in the graph G based on the structure of the incoming links. It was originally designed as an algorithm to rank web pages.
	'''
	pgrank = nx.pagerank(g)
	#print node_labels
	df = pd.DataFrame({ 'domain':[n.split("_")[0] for n in node_labels] ,'label': [n.split("_")[1] for n in node_labels],'clustcoeff':[clus_coeff.get(n,"NaN") for n in node_labels],'degree':[degree_cent.get(n,"NaN") for n in node_labels] ,'betweenness':[betw_cent.get(n,"NaN") for n in node_labels] ,'eigenvector':[eigenv_cent.get(n,"NaN") for n in node_labels], 'colseness':[closeness_cent.get(n,"NaN") for n in node_labels], 'pagerank':[pgrank.get(n,"NaN") for n in node_labels] })
	
	return df
	
def cosim_quartile(sim_mat):
	flat_sim = sim_mat[np.triu_indices(sim_mat.shape[0],k=1)]
	p4 = np.percentile(flat_sim, np.arange(25, 100, 25))
	return p4[2]

def network_analysis():
	start_time = time.time()
	sim_mat,labels = u.load_sim_mat()
	flat_sim = sim_mat[np.triu_indices(c.domain_topics["all"],k=1)]
	p10 = np.percentile(flat_sim, np.arange(10, 100, 10))
	p4 = np.percentile(flat_sim, np.arange(25, 100, 25))
	threshold = {"90p":p10[-1],"10p":p10[0],"75p":p4[-1],"25p":p4[0]}
	node_labels = domain_labels(labels)
	for t in threshold:
		print "========Creating Network for >",t,"threshold",threshold[t],"========"
		kwargs={"save": False,"gname":t}
		g = generate_network(sim_mat,labels,threshold[t],weighted=False,**kwargs)
		network_measures(g,labels,t)
		if t =="90p":
			plot_degree_dist(g)
			nx.write_gexf(g, "net/net_gexf.gexf")
	print "Network Analysis took ",(time.time()-start_time)/60," minutes"

