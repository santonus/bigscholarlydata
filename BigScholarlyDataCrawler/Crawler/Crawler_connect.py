import urllib2
import requests
import re
import string
from bs4 import BeautifulSoup as BS
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher

class Crawler_connect:

	def similar(self, a, b):
    		return SequenceMatcher(None, a, b).ratio()


	def ACM_establish_connection(self, title):	
		header = {'user-agent':'Mozilla/52.0'}
		for i in xrange(3):
			try:
				text = requests.get('http://dl.acm.org/results.cfm?query='+ title + '&filtered=&within=owners.owner=GUIDE&dte=&bfr=&srt=_score', headers=header).text
			except:
				continue
			break

		soup = BS(text, "html.parser")
		link =  soup.find('a',string = re.compile("^"+re.escape(title)+"$", re.I))

		try:
			url = 'http://dl.acm.org/' + link['href'] + '&preflayout=flat'
			for i in xrange(3):
				try:
					paper = requests.get(url, headers = header).text
				except:
					continue
				break

			soup = BS(paper, "html.parser")
			return soup

		except:
			return None


	def IEEE_establish_connection(self, title):
		header = {'user-agent':'Mozilla/52.0'}
		query = {'ti':title }
		for i in xrange(3):
			try:
				text = requests.get('http://ieeexplore.ieee.org/gateway/ipsSearch.jsp', params = query, headers=header).text
			except:
				continue
			break

		try:
			doc = ET.fromstring(text)
			document = doc.findall('document')
			for doc in document:
				title_IEEE = doc.find('title').text
				if self.similar(title, title_IEEE) > 0.88:
					return doc
			return None 

		except:	
			return None


	def Springer_establish_connection(self, title):
		query = {'query':title }
		header = {'user-agent':'Mozilla/52.0'}
		for i in xrange(3):
			try:
				text = requests.get('http://link.springer.com/search', params = query, headers=header).text
			except:
				continue
			break

		soup = BS(text, "html.parser")
		link =  soup.find("a", string = re.compile("^"+re.escape(title)+"$", re.I))

		try:
			url = 'http://link.springer.com/' + link['href']
			for i in xrange(3):
				try:
					paper = requests.get(url, headers = header).text
				except:
					continue
				break

			soup = BS(paper, "html.parser")
			return soup

		except:
			return None


	def establish_connection(self, title, website):
		if website.lower() == 'acm':
			 return self.ACM_establish_connection(title)
		elif website.lower() == 'ieee':
			 return self.IEEE_establish_connection(title)
		elif website.lower() == 'springer':
			return self.Springer_establish_connection(title)
		else:
			return None
