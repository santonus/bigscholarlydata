import string
from bs4 import BeautifulSoup as BS
import xml.etree.ElementTree as ET

class Crawler_venue:

	def get_venue_ACM(self, soup):
		try:
			venue = soup.find('meta', {'name': 'citation_conference'})['content']

		except:
			venue = None

		finally:
			return venue


	def get_venue_IEEE(self, document):
		try:
			return document.find('pubtitle').text

		except:
			return None


	def get_venue_Springer(self, soup):
		try:
			venue = soup.find('meta', {'name': 'citation_journal_title'})['content']

		except:	
			venue = None

		finally:
			return venue

