DBUSER='bigdataproj'
DBPWD='bigdataproj@123'
DBHOST='n12'
DB='data_AM'

domains = ["os","se","db","ai"]
threshold_quartile = 8 #{0: 25-100,1:50-100,2:75-100}
query_name = "soup_1k"
domain_topics={"se": 40,"ai":60,"db":40,"os":30,"all":170,"soup_60k":170,"soup_20k":170,"soup_1k":10}
#num_all = sum(domain_topics[domain] for domain in domains)
#domain_topics["all"] = num_all

#------------------LDA config------------------------
max_keywords = 1000
ngram_range = (1,3)
#---------------------Heatmap config-----------------
colours={"os":"red","se":"blue","db":"green","ai":"orange"}
#------------------- High corr terms for each RHL -------------
keep ={ 'pub':['colseness.intra',
'med.hindex','eigenvector.intra','papers','authors','pagerank.intra','degree.intra','rhl.pub'], 'cit':['rhl.cit','authors','papers','colseness.inter','eigenvector.inter','degree.inter','pagerank.inter','degree.intra','eigenvector.intra','colseness.intra','clustcoeff.intra','venues','betweenness.inter','clustcoeff.inter']}
#----------stats model --------------------
domain_cord={"ai":(0,0,0),"db":(1,0,0),"os":(0,1,0),"se":(0,0,1)}

