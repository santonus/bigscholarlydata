import string
from bs4 import BeautifulSoup as BS
import xml.etree.ElementTree as ET

class Crawler_author:

	def get_author_ACM(self, soup):
		try:
			author = ''
			authors = str(soup.find('meta', {'name': 'citation_authors'})['content'])
			print authors			
			author_list = authors.split(';')
			print author_list
			for a in author_list:
				for half in reversed(a.split(',')):
					author += half + ' '
				author += ';'

		except:
			author = None

		finally:
			if author == '':
				return None
			return author


	def get_author_IEEE(self, document):
		try:
			authors = document.find('authors').text
			if authors is None:
				return None
			else:
				return authors+';'

		except:
			return None


	def get_author_Springer(self, soup):
		try:
			authors = soup.find_all('meta', {'name': 'citation_author'})			
			author = ''
			for a in authors:
				author += a['content'] + ';'		

		except:	
			author = None

		finally:
			return author	

