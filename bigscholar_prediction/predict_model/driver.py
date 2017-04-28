import time
import predict_topic_cites as p
import corpus as c
import config as s
start_time = time.time()


for domain in s.domains:
	if domain not in c.all_domains:
		print "Invalid Domain: ",domain," doesn't exist in the database, please choose from ",",".join(c.all_domains)
	else:
		start_year,end_year = c.pubyr_range(domain)
		p.topic_cite_model(domain,start_year,end_year)


print ("--- Total Time: "+str((time.time() - start_time)/60)+" minutes ---") 

