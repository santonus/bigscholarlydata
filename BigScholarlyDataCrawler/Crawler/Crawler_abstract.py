import string
from bs4 import BeautifulSoup as BS
import xml.etree.ElementTree as ET
import re

class Crawler_abstract:
	
	def get_abstract_ACM(self, soup):
		try:
			abstract_lines = soup.find("a", string = "ABSTRACT").parent.next_sibling.next_sibling.contents[1].contents	
			abstract = ''
			for line in abstract_lines:
				line = re.sub('<.*?>','',str(line))
				for c in string.punctuation:
			  		line=line.replace(c,'')
				abstract += (line + ' ')
			abstract = abstract.encode('utf-8').strip()

		except:
			try:
				abstract = soup.find("a", string = "ABSTRACT").parent.next_sibling.next_sibling.contents[1].contents[0]
				abstract = abstract.encode('utf-8').strip()
				abstract = re.sub('<.*?>','',str(abstract))
			except:
				abstract = None

		finally:
			return abstract


	def get_abstract_IEEE(self, document):
		try:
			return document.find('abstract').text

		except:
			return None


	def get_abstract_Springer(self, soup):
		try:
			abstract = soup.find("h2", string = "Abstract").next_sibling.contents
			abstract = ' '.join(map(str, abstract))
			abstract = re.sub('<.*?>','',abstract)
			for c in string.punctuation:
				abstract=abstract.replace(c,'')

		except:	
			abstract = None

		finally:
			return abstract	
