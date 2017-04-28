DBHOST ="n12"
DBUSER = "bigdataproj"
DBPWD = "bigdataproj@123"
DB = "data_AM"

data_floor = 5 #minimum no of unique data points to be present in raw data 
pubyr_limit = 2003 #only papers published before it are considered
train_limit=2008 #year at which train phase ends
num_topics = 60
m = 100 # constant in model equation
domains = ["se"] # list of domains to run on