import mysql.connector
from mysql.connector import Error

class Filter_database:
	
	def __init__(self, sql_cnx):
		self.sql_cnx = sql_cnx		


	def filter_database(self):
		try:
			cursor= self.sql_cnx.cursor()
			query = 'create table dblp_pub_se like dblp_pub_all'
			cursor.execute(query)
			self.sql_cnx.commit()
			query = 'create table Aminer_pub_se like Aminer_pub_all'
			cursor.execute(query)
			self.sql_cnx.commit()
			cursor.close() 
			cursor1 = self.sql_cnx.cursor(buffered = True)
			cursor2 = self.sql_cnx.cursor()
			query = 'select venue_fullname, venue_shortname from Venues'
			cursor1.execute(query)
			for (venue_fullname, venue_shortname) in cursor1:
				query1 = 'insert into dblp_pub_se select * from dblp_pub_all where journal like "%' + venue_shortname +'%"'
				cursor2.execute(query1)
				query1 = 'insert into dblp_pub_se select * from dblp_pub_all where journal like  "%' + venue_fullname +'%"'
				cursor2.execute(query1)
				query1 = 'insert into Aminer_pub_se select * from Aminer_pub_all where journal like "%' + venue_shortname +'%"'
				cursor2.execute(query1)
				query1 = 'insert into Aminer_pub_se select * from Aminer_pub_all where journal like  "%' + venue_fullname +'%"'
				cursor2.execute(query1)
				self.sql_cnx.commit()
			cursor2.close()
			cursor1.close()				

		except Error as e:
			print e


	def filter_Aminer_citation_database(self):
		try:
			cursor= self.sql_cnx.cursor()
			query = 'create table cite_se2 like cite_all'
			cursor.execute(query)
			self.sql_cnx.commit()
			query = 'insert into cite_se2 select * from cite_all where index_id in (select index_id from Aminer_pub_se);'
			cursor.execute()	
			self.sql_cnx.commit(query)	

		except Error as e:
			print e

		finally:
			cursor.close()


	def extract_new_data(self):
		try:
			cursor= self.sql_cnx.cursor()
			query = 'create table pub_delta like Aminer_pub_se; insert into pub_delta select * from Aminer_pub_se where title not in (select title from pub_se); alter table pub_delta add ee varchar(64);insert into pub_delta(title, author, author_sequence_number, year, venue, ee) select title, author, author_sequence_number, year, journal, ee from dblp_pub_se where title not in (select title from pub_se); alter table pub_delta change column ee doi varchar(64);'
			cursor.execute(query)
			self.sql_cnx.commit()
		
		except Error as e:
			print e
		
		finally:
			cursor.close()	
