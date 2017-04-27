import string
from bs4 import BeautifulSoup as BS
import xml.etree.ElementTree as ET

class Crawler_doi:

	def get_doi_ACM(self, soup):
		try:
			doi = soup.find('meta', {'name': 'citation_doi'})['content']

		except:
			doi = None

		finally:
			return doi


	def get_doi_IEEE(self, document):
		try:
			return document.find('doi').text

		except:
			return None


	def get_doi_Springer(self, soup):
		try:
			doi = soup.find('meta', {'name': 'citation_doi'})['content']

		except:	
			doi = None

		finally:
			return doi	
