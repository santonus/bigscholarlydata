import numpy as np
from gensim import utils, corpora, matutils
from gensim.utils import check_output
from subprocess import PIPE
from corpus import *
# corpus saved in the blei format on disk
# dictionary to be saved in blei format on disk
# run shell command on script

def dim_corpus(corpus, time_slices):
	print("Creating DIM corpus")
	blei_corpus = corpora.BleiCorpus.serialize('dim-mult.dat', corpus)
	sequence = open("dim-seq.dat","w")
	sequence.write(str(len(time_slices))+"\n")
	for time in time_slices:
		sequence.write(str(time) + "\n")

def dim(dtm_path, input_dir, output_dir, num_topics=40):

	print("Running DIM")

	command = "--mode=fit     --rng_seed=0     --model=fixed     --initialize_lda=true     --corpus_prefix=example/test     --outname=example/output     --time_resolution=2     --influence_flat_years=5     --top_obs_var=0.5     --top_chain_var=0.005     --sigma_d=0.0001     --sigma_l=0.0001     --alpha=0.01     --lda_sequence_min_iter=6     --lda_sequence_max_iter=20     --save_time=-1     --ntopics=10     --lda_max_em_iter=10"
	command = command.split()
	command[4] = "--corpus_prefix=" + input_dir + "/dim"
	command[5] = "--outname=" + output_dir
	command[16] = "--ntopics=" + str(num_topics)
	command.insert(0, dtm_path)
	check_output(command)

	print("Done with DIM")

def dim_db(db, corpus, dict_id, time_slices, output_dir, num_topics=40):
# slices = [21, 447, 627, 2057, 3495, 6013, 6556, 10207, 10814]
	print(" Starting DIM to DB")

	fout_influence = output_dir + "/lda-seq/influence_time-{i}"
	influences_time = []
	for k, t in enumerate(time_slices):
	    stamp = "%03d" % k
	    print stamp, t
	    influence = np.loadtxt(fout_influence.format(i=stamp))
	    influence.shape = (t, num_topics)
	    print(influence.shape)
	    # influence[2,5] influence of document 2 on topic 5
	    influences_time.append(influence)

	cur = db.cursor()
	cur.execute("drop table if exists Document_Influence")
	cur.execute("CREATE TABLE if not exists Document_Influence(paper_id varchar(255) NOT NULL,topic_id varchar(255) NOT NULL, influence_prob DOUBLE(6,6), PRIMARY KEY (paper_id,topic_id))")
	cur.execute("delete from Document_Influence")
	doc_no = 0
	for i in range(0,len(time_slices)):
		for j in range(0, time_slices[i]):
			for k in range(0,num_topics):
				paper_id = dict_id[doc_no]
				topic_id = str(k)
				# probability of influence of jth doc to kth topic in time slice i
				influence_prob = str(influences_time[i][j][k])
				cur.execute("insert into Document_Influence(paper_id,topic_id,influence_prob) values (\""+paper_id+"\",\""+topic_id+"\",\""+influence_prob+"\");")
			doc_no += 1
	print(doc_no)		
	db.commit()

