This file provides information about running the LDA-DTM Topic Model

LDA:
input:
modify configurations in the ./topicmodel/driver.pys
Take care to input the appropriate load and num_topic paramters.
output:
Output will be stored in a db as specified in ./topicmodel/topics_db.py

DTM:
Before running DTM, you may have to compile the C++ code to generate binaries (main file).
Do this in ./dtm_release/dtm/ using make command.


WARNING:
Sum of probs over a paper may be greater than 1. This is a bug with the topic model from gensim / blei's code. To deal with this bug we have currently removed the 0.001 topics from the papers which have sum(prob) > 1.