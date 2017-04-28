import mysql.connector
from topic_model import *
from corpus import *
from topics_db import *
from gensim import corpora
from DIM import *

# CONSTANTS FOR XEON/NVIDIA
host="n12" 		 # your host, usually localhost		 	 
user="bigdataproj"        	 # username			
passwd="bigdataproj@123" 	 # password			
database="data_AM"		# name of the data base	
num_topics=80				# number of LDA/DTM/DIM topics
iterations=100				# number of LDA iterations.
dtm_path = "../dtm_release/dtm/main"  # path to DTM Blei binary
year_slices = 10 	# set of year slices for DTM
input_dir = "."    # input directory where you run the code from.
output_dir = "./output"				# code where you want DIM results to go
start_year=1975
end_year = 2015

#create database object using constants above
db = mysql.connector.connect(host=host, user=user, passwd=passwd, db=database)

'''
#Following is code to run the LDA model and to set up the topics in DB
model_lda = blei_lda(corpus, dictionary, num_topics=num_topics, iterations=iterations, load=True)
blei_lda_db(model_lda, db, corpus, dict_id ,start_year,end_year, num_topics=num_topics)
'''

# Following is the code to run the DTM model and to set up the topics in DB
# If you are doing DTM, have to run methods to set up time_slices.
time_slices, time_ranges = time_slices(db, year_slices)
corpus, dictionary, dict_id,time_slices = setup_corpus(db,time_ranges=time_ranges,time_slices=time_slices)
model_dtm = dtm(dtm_path, corpus, dictionary, time_slices, num_topics=num_topics, load=False)
#dtm_db(model_dtm, db, corpus, dict_id, time_ranges, num_topics=num_topics)

# DIM code
#dim_corpus(corpus, time_slices)
#dim(dtm_path, input_dir, output_dir, num_topics=num_topics)
#dim_db(db, corpus, dict_id, time_slices, output_dir, num_topics=num_topics)
