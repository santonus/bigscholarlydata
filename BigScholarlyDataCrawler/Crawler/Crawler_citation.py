import re
import string
import urllib2
import requests
from bs4 import BeautifulSoup as BS
import xml.etree.ElementTree as ET

class Crawler_citation:

	def get_citation_ACM(self, soup):
		try:
			refer_link_lines = soup.find("a", string = "REFERENCES").parent.next_sibling.next_sibling.find_all('a')
			refer_links = []
			for line in refer_link_lines:
				if line['href'] is not None:
					refer_links.append(line['href'])
			refer_title = ''
			for link in refer_links:
				url = 'http://dl.acm.org/' + link + '&preflayout=flat'
				for i in xrange(3):
					try:
						paper = requests.get(url, headers = header).text
					except:
						continue
					break

				soup = BS(paper, "html.parser")
				refer = soup.find('title').string
				if '404' not in refer:
					refer_title += refer.strip() + ';;;'

		except:
			refer_title = None
		
		finally:
			if refer_title == '':
				return None
			return refer_title

	def get_citation_IEEE(self, document):
		try:
			arnumber = document.find('arnumber').text
			if arnumber is not None:
				for i in xrange(3):
					try:
						text = requests.get('http://ieeexplore.ieee.org/xpl/dwnldReferences?arnumber='+arnumber).text
					except:
						continue
					break
				reference_list = re.split(r'([0-9]{1,3}\.)',text)
				reference_list = reference_list[1:-1]
				reference_list =  [refer for refer in reference_list if len(refer) > 5]
				reference = []
				for refer in reference_list:
					line = refer.split('\n\t\t\n\t\t')
					try:
						for i in xrange(len(refer)):
							if len(line[i]) == 0 or '</br>' in line[i]:
								continue
							if '.' not in line[i]:
								refined_line = re.sub(r'&.+?;', '', line[i])
								reference.append(refined_line)
								break
					except:
						continue
				refer_title = ''
				for refer in reference:
					refer = refer.replace('\r\n','')
					refer_title += refer.strip() + ';;;'
		
			else:
				refer_title = None
		except:
			refer_title = None
		
		finally:
			return refer_title

		
	def get_citation_Springer(self, soup):
		try:
			references_raw_list = soup.find("h3", string = "References" ).next_sibling.next_element.contents
			refer_title = ''
			for references_raw in references_raw_list:
				refer = re.search('\.:(.*?)\.', str(references_raw))
				if refer is None:
					refer = references_raw
					refer = re.sub('<.*?>','',str(refer))	
					refer_title += (refer.strip()+';;;')
				refer = refer.group(1).strip()
				refer_title += (refer + ';;;')
			return refer_title

		except:
			return None
