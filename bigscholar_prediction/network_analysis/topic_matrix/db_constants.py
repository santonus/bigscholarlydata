import mysql.connector

DBHOST ="n12"
DBUSER = "bigdataproj"
DBPWD = "bigdataproj@123"
DB = "data_AM"

#create connection with db
def dbConnect():
	cnx = mysql.connector.connect(user=DBUSER, password=DBPWD, host=DBHOST,database=DB)
	cursor = cnx.cursor()
	return cnx,cursor
