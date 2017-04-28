import mysql.connector
from nltk.stem.porter import *
from gensim import corpora


def time_slices(db, year_slices=5):
	print("Creating Time-Slices")
	cur = db.cursor()
	# getting min and max years to make time slices
	'''
	cur.execute("select min(pub_year),max(pub_year) from pub_se where paper_abstract <> \'\' and paper_published_year <> '1899'")
	min_year,max_year = cur.fetchall()[0]
	'''
	min_year,max_year = 1975,2015
	# adding 6 years to min_year because very few papers published then
	#min_year += 6
	time_ranges = []
	'''
	for i in range(min_year, max_year, year_slices):
		time_ranges.append((i, i + year_slices))
	'''
	for i in range(min_year, max_year, year_slices):
		time_ranges.append((min_year, i + year_slices))
	# manually check last time slice and increase if a few years are missed out. e.g given below:
	if time_ranges[-1][1] < max_year:
		time_ranges[-1][1] = max_year

	time_slices = []

	for min_y, max_y in time_ranges:
		
		cur.execute("select count(*) from pub_se where abstract <> \'\' and pub_year < "+ str(max_y)+" and pub_year > "+str(min_y))
		time_slices.append(int(cur.fetchall()[0][0]))
		
		#time_slices.append(100)

	return time_slices, time_ranges

def _removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

def setup_corpus(db,start_year=None,end_year=None, time_ranges=None, time_slices= None):
	print("Setting up corpus")
	# creates corpus, dictionary of words, and a mapping of paper ids to use later
	cur = db.cursor()
	# create stopwords through stopwords text file
	stopwords = open("stopwords.txt").read().split("\n")
	stemmer = PorterStemmer()
	corpus = []
	id_dict = {}
	i = 0
	if time_ranges is None:
		cur.execute("select paper_id, abstract from pub_se where pub_year <="+str(end_year)+  " AND pub_year >="+str(start_year)+" AND abstract <> '' ")
		for paper_id, abstract in cur.fetchall():
			# removing non-ascii characters
			abstract = _removeNonAscii(abstract)
			# removing stopwords
			abstract = [words for words in abstract.lower().split() if words not in stopwords]
			# stemming words			
			abstr = [stemmer.stem(words) for words in abstract]
			if len(abstr) < 5:
				continue
			corpus.append(abstr)
			id_dict[i] = paper_id
			i = i + 1

		dictionary = corpora.Dictionary(corpus)
		dictionary.save('corpus_dict.dict')
		corpus = [dictionary.doc2bow(text) for text in corpus]
		corpora.MmCorpus.serialize('corpus.mm', corpus)
		return corpus, dictionary, id_dict, time_slices

	elif time_ranges is not None:
		time = 0
		for min_y, max_y in time_ranges:
			#cur.execute("select paper_id, abstract from pub_se where abstract <> \'\' and pub_year < "+ str(max_y)+" and pub_year > "+str(min_y)+" limit 100")
			cur.execute("select paper_id, abstract from pub_se where abstract <> \'\' and pub_year < "+ str(max_y)+" and pub_year > "+str(min_y))
			for paper_id, abstract in cur.fetchall():
				# removing non-ascii characters
				abstract = _removeNonAscii(abstract)
				# removing stopwords
				abstract = [words for words in abstract.lower().split() if words not in stopwords]
				# stemming words
				abstr = [stemmer.stem(words) for words in abstract]
				if len(abstr) < 5:
					time_slices[time] = time_slices[time] - 1
					continue
				corpus.append(abstr)
				id_dict[i] = paper_id
				i = i + 1		
			time += 1

		# create term to word dictionary
		dictionary = corpora.Dictionary(corpus)
		# save the dictionary to disk
		dictionary.save('corpus_dict.dict')
		corpus = [dictionary.doc2bow(text) for text in corpus]
		# save the corpus to disk. If you wish to save to current directory to load, do so.
		corpora.MmCorpus.serialize('corpus.mm', corpus)
		return corpus, dictionary, id_dict, time_slices


def load_corpus():
	# if you have previously saved corpus to disc, directly load from here
	corpus = corpora.MmCorpus('corpus.mm')
	dictionary = corpora.dictionary.Dictionary.load('corpus_dict.dict')	
	return corpus, dictionary
