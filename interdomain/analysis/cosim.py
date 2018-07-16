import matplotlib.pyplot as plt
import common.utilities as u
import numpy as np
import seaborn as sns
sns.set()
import common.db
import common.config as c
import time

def get_topic_labels():
	conn,cursor = db.dbConnect()
	labels = {domain:{} for domain in c.domains}
	labels = [""]*sum(c.domain_topics[domain] for domain in c.domains)
	for domain in c.domains:
		sql = db.get_topic_labels(domain)
		cursor.execute(sql)
		rows = cursor.fetchall()
		for row in rows:
			labels[u.sim_mat_indexer(domain,u.fmt_tid(row[0]))] =row[1]
	conn.close()
	return labels

def generate_sim_matrix():
	dot = {}

	start_time = time.time()
	n = sum(c.domain_topics[domain] for domain in c.domains)
	sim_matrix = np.zeros((n,n))
	conn,cursor = db.dbConnect()
	cursor.execute('set global max_allowed_packet=671088640000')
	#dot product
	for d1 in c.domains:
		for d2 in c.domains[c.domains.index(d1):]:
			print "calculating ",d1,"X",d2
			for t1 in xrange(0,c.domain_topics[d1]):
				for t2 in xrange(0,c.domain_topics[d2]):
					i1 = u.sim_mat_indexer(d1,t1)
					i2 = u.sim_mat_indexer(d2,t2)
					#old way
					# if sim_matrix[i1][i2] ==0:
					# 	sql = db.keyword_dot(d1,"Topic_"+str(t1),d2,"Topic_"+str(t2))
					# 	cursor.execute(sql)
					# 	rows = cursor.fetchall()
					# 	for row in rows:
					# 		mul = float(row[0])
					# 		sim_matrix[i1][i2] = sim_matrix[i1][i2] + mul
					# 	if i1!=i2:
					# 		sim_matrix[i2][i1] = sim_matrix[i2][i1] + mul

					if sim_matrix[i1][i2] ==0:
						sql = db.keyword_dot(d1,"Topic_"+str(t1),d2,"Topic_"+str(t2))
						cursor.execute(sql)
						rows = cursor.fetchall()
						for row in rows:
							mul = float(row[0])
							sim_matrix[i1][i2] = sim_matrix[i1][i2] + mul
						if i1!=i2:
							sim_matrix[i2][i1] = sim_matrix[i2][i1] + mul

	
	'''
	#OLD METHOD	
	sql = db.get_topic_cross(d1,d2)
	print sql
	
	cursor.execute(sql)
	rows = cursor.fetchall()
	print "rows:",len(rows)
	#print "SQL query takes ",(time.time()-t1)/60,"mins"
	
	for row in rows:
		i1 = u.sim_mat_indexer(d1,u.fmt_tid(row[0]))
		i2 = u.sim_mat_indexer(d2,u.fmt_tid(row[1]))
		mul = float(row[2])
		sim_matrix[i1][i2] = sim_matrix[i1][i2] + mul
		if i1!=i2: 
			sim_matrix[i2][i1] = sim_matrix[i2][i1] + mul
	'''
	conn.close()
	cursor.close()
	conn,cursor = db.dbConnect()
	# divide by mod
	for d1 in c.domains:
		sql = db.get_topic_l2(d1)
		t2 = time.time()
		cursor.execute(sql)
		print "SQL query takes ",(time.time()-t2)/60,"mins"
		rows = cursor.fetchall()
		for row in rows:
			t = u.sim_mat_indexer(d1,u.fmt_tid(row[0]))
			mod = np.sqrt(float(row[1]))
				
			if mod!= 0:
				sim_matrix[t,:] /= mod
				sim_matrix[:,t] /= mod
			else:
				print "ZERO!!!"
	conn.close()

	print "--- time for sim matrix",sim_matrix.shape," generation "+str((time.time() - start_time)/60)+" minutes ---"
	return sim_matrix

def plot_heatmap(sim_mat,labels):
	vmax = 0.25
	vmin = 0
	ax = sns.heatmap(sim_mat, cmap="YlGnBu",linewidth=0.005,vmin=vmin,vmax=vmax)
	#ax.xaxis.tick_top()
	y_labels = labels
	x_labels = labels
	ax.figure.set_size_inches(20,15)
	#Ticks for every topic
	#plt.xticks(np.arange(1,len(x_labels),1))
	#plt.yticks(np.arange(1,len(y_labels),1))
	
	# For labels
	#ax.set_xticklabels(x_labels,rotation=60,size=8)
	#ax.set_yticklabels(y_labels,rotation=30S,size=8)
	#ax.tight_layout()
	fig = ax.get_figure()

	#fig.suptitle("os vs os",fontsize=30)
	#ttl = ax.title("os vs os")
	#ttl.set_position([.5, 1.05])
	#ax.hlines([30], *ax.get_xlim(),color="blue")
	#ax.vlines([30], *ax.get_ylim(),color="blue")
	#ax.hlines([70], *ax.get_xlim(),color="blue")
	#ax.vlines([70], *ax.get_ylim(),color="blue")
	#ax.hlines([110], *ax.get_xlim(),color="blue")
	#ax.vlines([110], *ax.get_ylim(),color="blue")

	
	#print "x_labels",len(x_labels),"y_labels",len(y_labels)
	#print "x_colours",len(x_colours),"y_colours",len(y_colours)
	if len(c.domains) > 1:
		y_colours = [c.colours[d1]]*c.domain_topics[d1]
		x_colours = [c.colours[d2]]*c.domain_topics[d2]
		for y_colour, y_tick in zip(y_colours,ax.yaxis.get_major_ticks()):
			y_tick.label1.set_verticalalignment("bottom")
			y_tick.label1.set_color(y_colour)

		for x_colour,x_tick in zip(x_colours,ax.xaxis.get_major_ticks()):
			x_tick.label1.set_horizontalalignment("right")
			x_tick.label1.set_color(x_colour)
	return ax
				
def heatmap():
	print "generating sim mat"
	#sim_mat = generate_sim_matrix()
	#topic_labels = get_topic_labels()
	#print "saving sim_mat,labels in ./sim_mat"
	#np.savez(open("sim_mat_"+c.query_name,"wb"),sim_mat=sim_mat,labels=topic_labels)
	#333333333 DELETE THE FOLLOWIN LATER 3333333333
	data = np.load("sim_mat_"+c.query_name)
	sim_mat = data['sim_mat']
	topic_labels=data['labels']
	
	ax = plot_heatmap(sim_mat,topic_labels).get_figure()
	ax.savefig("heatmap_"+c.query_name+".png")
	
