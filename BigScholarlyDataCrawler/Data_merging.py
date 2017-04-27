import mysql.connector
from mysql.connector import Error

class Merge_database:
	
	def __init__(self, sql_cnx):
		self.sql_cnx = sql_cnx		


	def merge_database(self):
		try:
			cursor= self.sql_cnx.cursor()
			query = 'SELECT @max := MAX(paper_id)+ 1 FROM pub_se; PREPARE stmt FROM "ALTER TABLE pub_se AUTO_INCREMENT = ?";EXECUTE stmt USING @max;DEALLOCATE PREPARE stmt;'
			cursor.execute(query)
			self.sql_cnx.commit()
			query = 'insert into pub_se(title, year, doi, venue, abstract) select title, year, doi, venue, abstract from pub_delta;'		
			cursor.execute(query)
			self.sql_cnx.commit()

		except Error as e:
			print e

		finally:
			cursor.close()


	def merge_citation(self):
		try:
			cursor= self.sql_cnx.cursor()
			query = 'delete from cite_new where refer_title not in (select title from pub_delta);'
			cursor.execute(query)
			self.sql_cnx.commit()
			query = 'insert into cite_se select P1.paper_id, P2.paper_id from pub_se P1 inner join cite_new C on P1.title = C.title inner join pub_se P2 on C.refer_title = P2.title;'
			cursor.execute(query)
			self.sql_cnx.commit()

		except Error as e:
			print e

		finally:
			cursor.close()
	

	def merge_authors(self):
		try:
			cursor= self.sql_cnx.cursor()
			query = 'SELECT @max := MAX(auhtor_id)+ 1 FROM Authors; PREPARE stmt FROM "ALTER TABLE Authors AUTO_INCREMENT = ?";EXECUTE stmt USING @max;DEALLOCATE PREPARE stmt;'
			cursor.execute(query)
			self.sql_cnx.commit()
			query = 'insert into Auhtors(author_name, author_h_index, author_p_index_eqAindex, author_p_index_uneqAindex, author_keywords) select author_name, author_h_index, author_p_index_eqAindex, author_p_index_uneqAindex, author_keywords from Auhtor_all where auhtor_name in (select author from pub_delta);'		
			cursor.execute(query)
			self.sql_cnx.commit()

		except Error as e:
			print e

		finally:
			cursor.close()


	def merge_affiliations(self):
		try:
			cursor= self.sql_cnx.cursor()
			query = 'SELECT @max := MAX(affiliation_id)+ 1 FROM Affiliations; PREPARE stmt FROM "ALTER TABLE Affiliations AUTO_INCREMENT = ?";EXECUTE stmt USING @max;DEALLOCATE PREPARE stmt;'
			cursor.execute(query)
			self.sql_cnx.commit()
			query = 'insert into Affiliations(affiliation_name) select affliation_name from Auhtor_all where auhtor_name in (select author from pub_delta);'		
			cursor.execute(query)
			self.sql_cnx.commit()

		except Error as e:
			print e

		finally:
			cursor.close()


	def merge_paper_author_affiliations(self):
		try:
			cursor= self.sql_cnx.cursor()
			self.sql_cnx.commit()
			query = 'insert into Paper_Author_Affiliations select P.paper_id, A1.author_id, A2.affiliation_id, P1.author_sequence_number from pub_se P inner join pub_delta P1 on P.title = P1.title inner join Authors A1 on P1.author = A1.author_name inner join Affiliations A2 on A1.affiliation_name = A2.affiliation_name;'		
			cursor.execute(query)
			self.sql_cnx.commit()

		except Error as e:
			print e

		finally:
			cursor.close()
