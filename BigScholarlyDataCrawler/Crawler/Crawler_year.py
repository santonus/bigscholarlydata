import string
from bs4 import BeautifulSoup as BS
import xml.etree.ElementTree as ET

class Crawler_year:

	def get_year_ACM(self, soup):
		try:
			date = soup.find('meta', {'name': 'citation_date'})['content']
			year = date[-4:]

		except:	
			year = None

		finally:
			return year	



	def get_year_IEEE(self, document):
		try:
			return document.find('py').text

		except:
			return None


	def get_year_Springer(self,soup):
		try:
			date = soup.find('meta', {'name': 'citation_publication_date'})['content']
			year = date[:4]

		except:	
			year = None

		finally:
			return year	

