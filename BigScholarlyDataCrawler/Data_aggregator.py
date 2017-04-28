from __future__ import print_function
import xml.etree.ElementTree as ET
import mysql.connector
import os

class Update_raw_database:

	def __init__(self, sql_cnx):
		self.sql_cnx = sql_cnx
		# os.mkdir("temp")		

	def XML_file_cleaning(self, XML_filename):
		f = open(XML_filename, "r")
		f1 = open("./temp/cleaned.xml", "w")
		text = f.readlines()
		for i in xrange(len(text)-1):
			line = text[i]
			if "&" in line:
				a = line.find("&")
				b = line.find(";")
				line = line[:a] + line[a+1] + line[(b+1):]

			if "&" in line:
				a = line.find("&")
				b = line.find(";")
				line = line[:a] + line[a+1] + line[(b+1):]

			if "&" in line:
				a = line.find("&")
				b = line.find(";")
				line = line[:a] + line[a+1] + line[(b+1):]

			if "&" in line:
				a = line.find("&")
				b = line.find(";")
				line = line[:a] + line[a+1] + line[(b+1):]

			if "&" in line:
				line = "Null"
	
			if ";" in line:
				b = line.find(";")
				line = line[:b] + line[(b+1):]

			text[i] = line	
		f1.writelines(text)
		
	
	def DBLP_to_sqldump(self, XML_filename):
		self.XML_file_cleaning(XML_filename)
		log = open("./temp/dblp.sql", "w")
		print("drop table if exists dblp_pub_all;",file =log)
		print("create table dblp_pub_all(id int auto_increment primary key, author longtext, title longtext, month varchar(10), year varchar(10), journal varchar(255), ee varchar(255), volume varchar(100), author_sequence_number int);",file=log)
		tree = ET.ElementTree(file='./temp/cleaned.xml')
		root = tree.getroot()
		elem = ['title', 'month', 'year', 'journal', 'ee', 'volume']
		for article in root.findall('article'):
			insert = {'author':'NULL', 'title':'NULL','month':'NULL','year':'NULL','journal':'NULL','ee':'NULL','volume':'NULL', 'author_seq':'NULL'}
			for element in article:
				if element.tag in elem:
					if element.text is None:
						text = 'NULL';
					else:
						text = element.text
					if '"' in text:
						text = text.replace('"','\\"')
					if ';' in text:
						text = text.replace(';','\\;')
					insert[element.tag] = text
			seq = 0
			for author in article.iter('author'):
				insert['author'] = author.text
				seq += 1
				print( 'insert into dblp_pub_all(author, title, month, year, journal, ee, volume, author_sequence_number) values("',insert['author'],'","', insert['title'],'","', insert['month'],'","', insert['year'],'","', insert['journal'],'","', insert['ee'],'","', insert['volume'], '",', seq,');', file = log) 
		for el in elem:
			print('update dblp_pub_all set ', el, '=NULL where ', el, "=' NULL ';", file=log)
		elem = ['title', 'month', 'year', 'journal', 'ee', 'volume', 'author']
		for el in elem:
			print('update dblp_pub_all set '+ el + ' = trim(' + el + ');', file = log)
	

	def Aminer_data_to_sqldump(self, Aminer_filename):
		file  = open(Aminer_filename,"r")
		file2 = open("./temp/Aminer.sql","w")
		text = file.read().split("\n\n")
		print("drop table if exists Aminer_pub_all;",file =file2)
		print("create table Aminer_pub_all( title longtext, author varchar(255), author_sequence_number int, year varchar(10), venue varchar(255), index_id varchar(255), abstract longtext);",file=file2)
		for sample in text:
			lines = sample.split("\n")
			authors = "NULL"
			title = "NULL"
			year = "NULL"
			venue = "NULL"
			abstract = 'NULL'
			index = 'NULL'
			for line in lines:
				if len(line) <= 1:
					continue
				line = line.replace('"','\\"')
				if line[1] == '*':
					title = line[2:]
				if line[1] == '@':
					authors = line[2:]
				if line[1] == 't':
					year = line[2:]
				if line[1] == 'c':
					venue = line[2:]
				if line[1] == 'i':
					index = line[6:]
				if line[1] == '!':
					abstract = line[2:]
			author_list = authors.split(',')
			seq = 0
			for author in author_list:
				seq += 1
				print('insert into Aminer_pub_all(title, author, author_sequence_number, year, venue, index_id, abstract) values("',title,'","',author,'",', seq, ',"',year,'","',venue,'","',index,'","',abstract,'");',file=file2)
		elem = ['title', 'author', 'year', 'venue', 'index_id', 'abstract']
		for el in elem:
			print('update Aminer_pub_all set ', el, '=NULL where ', el, "=' NULL ';", file=file2)
			print('update Aminer_pub_all set '+ el + ' = trim(' + el + ');', file = file2)


	def Aminer_citation_to_sqldump(self, Aminer_filename):
		file  = open(Aminer_filename,"r")
		file2 = open("./temp/citation.sql","w")
		text = file.read().split("\n\n")
		print("drop table if exists cite_all;",file =file2)
		print("create table cite_all(index_id varchar(255), reference_index varchar(255), primary key(index_id, reference_index));",file=file2)
		for sample in text:
			lines = sample.split("\n")
			ref = []#list of references
			index = 'NULL'
			for line in lines:
				if len(line) <= 1:
					continue
				if line[1] == 'i':
					index = line[6:]
				if line[1] == '%':
					reference = line[2:]
					ref.append(reference)
			if len(ref) == 0:
				ref.append("NULL")
			for i in xrange(len(ref)):
				print('insert into cite_all( index_id, reference_index) values("', index,'","', ref[i],'");',file=file2)

		print('delete from cite_all where reference_index = " NULL ";' , file = file2)
		elem = ['index_id', 'reference_index']
		for el in elem:
			print('update cite_all set '+ el + ' = trim(' + el + ');', file = file2)


	def Author_to_sqldump(self, Author_filename):
		file  = open(Author_filename,"r")
		file2 = open("./temp/Author.sql","w")
		text = file.read().split("\n\n")
		print("drop table if exists Author_all;",file =file2)
		print("create table Auhtor_all(author_name varchar(256), affiliation_name varchar(256) , author_h_index float, author_p_index_eqAindex float, author_p_index_uneqAindex FLOAT, author_keywords TEXT);",file=file2)
		for sample in text:
			lines = sample.split("\n")
			author_name = 'NULL'
			author_h_index = 0.0
			author_p_index_eqAindex = 0.0
			author_p_index_uneqAindex = 0.0
			author_keywords = 'NULL'
			affiliation_name = 'NULL'
			for line in lines:
				if len(line) <= 1:
					continue
				if line[1] == 'n':
					author_name = line[2:]
					continue
				if line[1] == 'a':
					affiliation_name = line[2:]
					continue
				if line[1] == 'h':
					author_h_index = line[3:]
					continue
				if line[1:3] == 'pi':
					author_p_index_eqAindex = line[3:]
					continue
				if line[1] == 'u':
					author_p_index_uneqAindex = line[4:]
					continue
				if line[1] == 't':
					author_keywords = line[2:]
					continue
			print('insert into Author_all( author_name, affiliation_name, author_h_index, author_p_index_eqAindex, author_p_index_uneqAindex, author_keywords) values ("'+author_name+'","'+affiliation_name+'",'+author_h_index+','+author_p_index_eqAindex+','+author_p_index_uneqAindex+',"'+author_keywords+'")', file = file2)
		elem = ['author_name', 'affiliation_name','author_keywords']
		for el in elem:
			print('update Author_all set ', el, '=NULL where ', el, "=' NULL ';", file=file2)
			print('update Auhtor_all set '+ el + ' = trim(' + el + ');', file = file2)


	def update_database(self):
		cursor = self.sql_cnx.cursor()
		for line in open('./temp/dblp.sql'):
			cursor.execute(line)
		for line in open('./temp/Aminer.sql'):
			cursor.execute(line)
		for line in open('./temp/citation.sql'):
			cursor.execute(line)
		for line in open('./temp/Author.sql'):
			cursor.execute(line)
		self.sql_cnx.commit()
		cursor.close()


	def update_venue_rank(self, CORE_filename):
		cursor = self.sql_cnx.cursor()
		query1 = 'create table venue_rank (venue_fullname, venue_shortname, rank, field-of_study1, field_of_study2, field_of_study3);'
		cursor.execute(query1)
		cnx.commit()
		query2 = 'load data infile' + CORE_filename + 'into table venue_rank fields terminated by "," lines terminated by "\n" '
		cursor.execute(query2)
		cnx.commit()
		cursor.close()
