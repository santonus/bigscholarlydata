from gensim.models.wrappers.dtmmodel import DtmModel
from gensim import corpora, models, similarities,matutils


def dtm(dtm_path, corpus, dictionary, time_slices, num_topics=40, load=False):
	# dtm_path should have your local binary of Blei-DTM
	print("Running DTM")
	if load is False:
		model = DtmModel(dtm_path, corpus, time_slices, num_topics=num_topics, id2word=dictionary,initialize_lda=True)
		model.save("DTM")
		return model
	elif load is True:
		model = DtmModel.load('DTM')
		return model

def blei_lda(corpus, dictionary, num_topics=80, iterations=50, load=False):
	print("Running Blei")
	if load is False:
		model = models.ldamodel.LdaModel(corpus, num_topics=num_topics,id2word=dictionary,iterations=iterations)
		model.save("LDA")
		return model
	elif load is True:
		model = models.LdaModel.load('LDA')
		return model