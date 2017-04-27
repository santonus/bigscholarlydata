import mysql.connector

class Database_connect:

	def __init__(self, username, password, host, dbname):
		self.db_username = username
		self.db_password = password
		self.db_host = host
		self.db_databasename = dbname
		self.cnx = None


	def connect(self):
		if self.cnx is None:
			self.cnx = mysql.connector.connect(user = self.db_username, password = self.db_password, host = self.db_host, database = self.db_databasename)
		return self.cnx

	def close_connection(self):
		self.cnx.close()
