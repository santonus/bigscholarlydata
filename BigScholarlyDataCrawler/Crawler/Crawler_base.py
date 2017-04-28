from __future__ import print_function
import mysql.connector
from mysql.connector import Error
import urllib2
import requests
import re
import string
from bs4 import BeautifulSoup as BS
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from Crawler_connect import Crawler_connect
from Crawler_abstract import Crawler_abstract
from Crawler_author import Crawler_author
from Crawler_citation import Crawler_citation
from Crawler_doi import Crawler_doi
from Crawler_venue import Crawler_venue
from Crawler_year import Crawler_year

class Crawler:

	def __init__(self, sql_cnx):
		self.sql_cnx = sql_cnx	
		os.chdir("temp")
		self.c_connect = Crawler_connect()
		self.c_abstract = Crawler_abstract()	
		self.c_author = Crawler_author()
		self.c_citation = Crawler_citation()		
		self.c_doi = Crawler_doi()
		self.c_venue = Crawler_venue()
		self.c_year = Crawler_year()	

		
	def get_titles_without_abstract(self):
		f = open('title_abs.txt', 'w')	
		try:
			cursor= self.sql_cnx.cursor()
			query = 'select distinct title from pub_delta where abstract is null;'
			cursor.execute(query)
			for title in cursor:
				print (str(title), file = f)
		
		except Error as e:
			print(e)
		
		finally:
			cursor.close()

	def abstarct_crawler(self, title_filename):
		f = open(title_filename, "r")
		f2 = open("abstract.txt", "a")
		titles= f.readlines()

		for title in titles:
			title = title[3:]
			title = title[:-4]
			if title[-1] == '.':
				title = title[:-1]	
			f2.write(title+'\n')
			handle = self.c_connect.establish_connection(title,"ACM")
			abstract = self.c_abstract.get_abstract_ACM(handle)
			
			if abstract is None:
				handle = self.c_connect.establish_connection(title, "IEEE")
				abstract = self.c_abstract.get_abstract_IEEE(handle)
				if abstract is None:
					handle = self.c_connect.establish_connection(title, "Springer")
					abstract = self.c_abstract.get_abstract_Springer(handle)
					if abstract is None:
						abstract = 'Null'
					
			abstract = abstract.replace('\r\n',' ')
			f2.write(str(abstract))
			f2.write(";;;\n")


	def update_abstract_database(self):
		try:
			cursor= self.sql_cnx.cursor()
			f = open("abstract.txt","r")
			paper = f.read().split(';;;')
			i = 0
			while i < len(paper):
				title = paper[i].split('\n')[1]
				title = title.replace("'","\\'")
				abstract = ''
				for line in paper[i].split('\n')[2:]:
					abstract += line
				abstract = abstract.replace("\n","")
				abstract = abstract.replace("'","\\'")
				if abstract.lower() == "null":
				    i += 1
				    continue

				try:
				    	query =( " UPDATE pub_delta SET abstract ='" + abstract + "' WHERE title ='" + title + "';")
				    	cursor.execute(query)
					self.sql_cnx.commit()
				
				except:
					pass
				i += 1

		except Error as e:
			print(e)
		
		finally:
			cursor.close()


	def get_titles_without_citation(self):
		f = open("title_citation.txt", "w")
		try:
			cursor= self.sql_cnx.cursor()
			query = 'select distinct title from dblp_pub_se where title not in ( select title from pub_se);'
			cursor.execute(query)
			for title in cursor:
				print (str(title), file = f)
		
		except Error as e:
			print(e)
		
		finally:
			cursor.close()			


	def citation_crawler(self, title_filename):
		f = open(title_filename, "r")
		f2 = open("refer.txt", "a")
		titles= f.readlines()

		for title in titles:
			title = title[3:]
			title = title[:-4]
			if title[-1] == '.':
				title = title[:-1]	
			f2.write(title+'\n')
	
			handle = self.c_connect.establish_connection(title,'ACM')
			refer_title = self.c_citation.get_citation_ACM(handle)
			if refer_title is None:
				handle = self.c_connect.establish_connection(title, "IEEE")
				abstract = self.c_citation.get_citation_IEEE(handle)
				if refer_title is None:
					handle = self.c_connect.establish_connection(title, "Springer")
					refer_title = self.c_citation.get_citation_Springer(handle)
					if refer_title is None:
						refer_title = 'Null'

			f2.write(str(refer_title))
			f2.write("\n")
					
					
	def update_citation_database(self):
		try:
			cursor= self.sql_cnx.cursor()
			query = 'create table cite_new ( title longtext not null primary key, refer_title longtext )'
			cursor.execute(query)
			self.sql_cnx.commit()
			f = open("refer.txt","r")
			paper = f.readlines()
			i = 0
			while i < len(paper):
				title = paper[i]
				title = title.replace("\n","")
				title = title.replace("'","\\'")
				title = title + '.'
				references = paper[i+1]
				if references.lower() == "null":
					    i += 2
					    continue
				references = references.split(';;;')
				references = references[:-1]
				try:
					for refer in references:
						refer = refer + '.'
						refer = refer.replace("'","\\'")
				    		refer = refer.replace(";","\\;")
				    		query =( "insert into cite_new values ('" + title + "', '" + refer + "');")
				    		cursor.execute(query)
				    		self.sql_cnx.commit()
				except:
					pass
				i += 2

		except Error as e:
			print(e)
		
		finally:
			cursor.close()	
			
			
	def get_titles_without_info(self):
		f = open("title_info.txt", "w")
		try:
			cursor= self.sql_cnx.cursor()
			query = 'select distinct refer_title from cite_new where refer_title not in ( select title from pub_se union select title from pub_delta);'
			cursor.execute(query)
			for title in cursor:
				print (str(title), file = f)
		
		except Error as e:
			print(e)
		
		finally:
			cursor.close()	


	def info_crawler(self, title_filename):
		f = open(title_filename, "r")
		f2 = open("info.txt", "a")
		titles= f.readlines()

		for title in titles:
			title = title[3:]
			title = title[:-4]
			if title[-1] == '.':
				title = title[:-1]	
			f2.write(title+'\n')

			handle = self.c_connect.establish_connection(title, 'ACM')
			abstract = self.c_abstract.get_abstract_ACM(handle)
			if abstract is None:
				handle = self.c_connect.establish_connection(title, "IEEE")
				abstract = self.c_abstract.get_abstract_IEEE(handle)
				if abstract is None:
					handle = self.c_connect.establish_connection(title, "Springer")
					abstract = self.c_abstract.get_abstract_Springer(handle)
					if abstract is None:
						abstract = 'Null'
			abstract = abstract.replace('\r\n',' ')
			f2.write(str(abstract))
			f2.write("\n")

			handle = self.c_connect.establish_connection(title, 'ACM')
			author = self.c_author.get_author_ACM(handle)
			if author is None:
				handle = self.c_connect.establish_connection(title, "IEEE")
				author = self.c_author.get_author_IEEE(handle)
				if author is None:
					handle = self.c_connect.establish_connection(title, "Springer")
					author = self.c_author.get_author_Springer(handle)
					if author is None:
						author = 'Null'
			author = author.replace('\r\n',' ')
			f2.write(str(author))
			f2.write("\n")

			handle = self.c_connect.establish_connection(title, 'ACM')
			year = self.c_year.get_year_ACM(handle)
			if year is None:
				handle = self.c_connect.establish_connection(title, "IEEE")
				year = self.c_year.get_year_IEEE(handle)
				if year is None:
					handle = self.c_connect.establish_connection(title, "Springer")
					year = self.c_year.get_year_Springer(handle)
					if year is None:
						year = 'Null'
			year = year.replace('\r\n',' ')
			f2.write(str(year))
			f2.write("\n")

			handle = self.c_connect.establish_connection(title, 'ACM')
			venue = self.c_venue.get_venue_ACM(handle)
			if venue is None:
				handle = self.c_connect.establish_connection(title, "IEEE")
				venue = self.c_venue.get_venue_IEEE(handle)
				if venue is None:
					handle = self.c_connect.establish_connection(title, "Springer")
					venue = self.c_venue.get_venue_Springer(handle)
					if venue is None:
						venue = 'Null'
			venue = venue.replace('\r\n',' ')
			f2.write(str(venue))
			f2.write("\n")

			handle = self.c_connect.establish_connection(title, 'ACM')
			doi = self.c_doi.get_doi_ACM(handle)
			if doi is None:
				handle = self.c_connect.establish_connection(title, "IEEE")
				doi = self.c_doi.get_doi_IEEE(handle)
				if doi is None:
					handle = self.c_connect.establish_connection(title, "Springer")
					doi = self.c_doi.get_doi_Springer(handle)
					if doi is None:
						doi = 'Null'
			doi = doi.replace('\r\n',' ')
			f2.write(str(doi))
			f2.write("\n")

		
	def update_info_database(self):
		try:
			cursor= self.sql_cnx.cursor()
			f = open("info.txt","r")
			paper = f.readlines()
			i = 0
			while i < len(paper):
				title = paper[i]
				title = title.replace("\n","")
				title = title.replace("'","\\'")
				title = title + '.'

				abstract = paper[i+1]
				abstract = abstract.replace("\n","")
				abstract = abstract.replace("'","\\'")

				author = paper[i+2]
				author = author.replace("\n","")
				author = author.replace("'","\\'")

				year = paper[i+3]
				venue = papaer[i+4]
				venue = venue.replace("\n","")
				venue = venue.replace("'","\\'")
				doi = paper[i+5]
				
				authors = author.split(';')
				authors = authors[:-1]
				
				if abstract.lower() == "null":
				    i += 6
				    continue

				try:
					j = 0
					for a in authors:
				    		query =( "insert into pub_delta(title, author, author_sequence_number, year, venue, abstract, doi) values ('" + title + "', '" + a.strip() + "',"+ i + ",'" + year + "','" + venue + "','" + abstract + "','" + doi + "');")
						j += 1
				    		cursor.execute(query)
				    		self.sql_cnx.commit()
				except:
					pass
				i += 6

		except Error as e:
			print(e)
		
		finally:
			cursor.close()
