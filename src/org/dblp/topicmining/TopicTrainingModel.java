package org.dblp.topicmining;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import com.csvreader.CsvWriter;

import cc.mallet.grmm.inference.GibbsSampler;
import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.topics.TopicAssignment;
import cc.mallet.types.Alphabet;
import cc.mallet.types.FeatureSequence;
import cc.mallet.types.IDSorter;
import cc.mallet.types.InstanceList;
import cc.mallet.types.LabelSequence;

/**
 * This class contains methods to build the topic model and output various training 
 * model files
 *
 */
public class TopicTrainingModel {

	private ParallelTopicModel topicModel;

	public void buildTopicModelUsingLDA(InstanceList trainingData, int numTopics, double alpha, double beta, int samplingIterations) throws IOException{

		if (trainingData.size() > 0 &&
				trainingData.get(0) != null) {
			Object data = trainingData.get(0).getData();
			if (! (data instanceof FeatureSequence)) {
				System.err.println("Topic modeling currently only supports feature sequences: use --keep-sequence option when importing data.");
				System.exit(1);
			}
		}

		topicModel = new ParallelTopicModel (numTopics, alpha, beta);
		if (TopicConstants.RANDOMSEED != 0) {
			topicModel.setRandomSeed(TopicConstants.RANDOMSEED);
		}
		topicModel.addInstances(trainingData);
		topicModel.setTopicDisplay(TopicConstants.SHOWTOPICSINTERVAL, TopicConstants.TOPWORDS);

		/*
            if (testingFile.value != null) {
                topicModel.setTestingInstances( InstanceList.load(new File(testingFile.value)) );
            }
		 */

		topicModel.setNumIterations(samplingIterations);
		topicModel.setOptimizeInterval(TopicConstants.OPTIMIZEINTERVAL);
		topicModel.setBurninPeriod(TopicConstants.OPTIMIZEBURNIN);
		topicModel.setSymmetricAlpha(TopicConstants.USESYMMETRICALPHA);
		topicModel.setNumThreads(TopicConstants.NUMTHREADS);
		topicModel.estimate();
	}

	public ParallelTopicModel getTopicModel(){
		return topicModel;
	}

	/**
	 * This method generates the required topic output files. The files generated are:
	 * 1. topic-keys.txt : This file contains the key words assigned to each of the topics
	 * 2. topic-state.gz: This zip file contains a file named topic-state which contains a list
	 * 					  of each token in the input files followed by the topic to which it 
	 * 					  has been assigned
	 * 3. topics-doc.txt: This file contains the name of the input text file followed by 
	 * 					  probabilities that this document may belong to each of the topics 
	 *                    
	 * @param topicModel
	 * @param folderPath
	 * @throws IOException
	 */
	public void generateTopicOutputFiles(ParallelTopicModel topicModel, String folderPath) throws IOException{

		topicModel.printTopWords(new File(folderPath+File.separator + TopicConstants.TOPICKWFILENAME), TopicConstants.TOPWORDS, false);

		topicModel.printState (new File(folderPath+File.separator + TopicConstants.TOPICSTATEFILENAME));

		PrintWriter out = new PrintWriter (new FileWriter ((new File(folderPath+File.separator + TopicConstants.TOPICDOCFILENAME))));
		topicModel.printDocumentTopics(out, TopicConstants.DOCTOPICSTHRESHOLD, TopicConstants.DOCTOPICSMAX);
		out.close();

		assert (topicModel != null);
		try {

			ObjectOutputStream oos =
				new ObjectOutputStream (new FileOutputStream (folderPath+ File.separator + TopicConstants.TOPICMODELFILENAME));
			oos.writeObject (topicModel);
			oos.close();

		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException ("Couldn't write topic model to filename "+folderPath+ File.separator + TopicConstants.TOPICMODELFILENAME);
		}
		
		ObjectOutputStream oos = 
			new ObjectOutputStream(new FileOutputStream(folderPath+File.separator+TopicConstants.evaluatorFile));
		oos.writeObject(topicModel.getProbEstimator());
		oos.close();
	}

	/**
	 * This is a temporary method to calculate the likelihood instead of using the likelihood given
	 * by Mallet
	 * @param topicModel
	 * @return
	 */
	public double getLogLikelihood(ParallelTopicModel topicModel){

		double logLikelihood= 0.0;

		TreeSet[] topicSortedWords = topicModel.getSortedWords();
		Alphabet alphabet= topicModel.getAlphabet();
		ArrayList<HashMap<Integer, Double>> phi = new ArrayList<HashMap<Integer,Double>>(0);
		ArrayList<HashMap<Integer, Double>> theta = new ArrayList<HashMap<Integer,Double>>(0);
		// Extract the probabilities of words per topic
		for (int topic = 0; topic < topicModel.getNumTopics(); topic++) {
			TreeSet<IDSorter> sortedWords = topicSortedWords[topic];
			Iterator<IDSorter> iterator = sortedWords.iterator();

			HashMap<Integer, Double> topicData= new HashMap<Integer, Double>(0);
			double totalWeight=0;

			// Populating the hash map with the ids and weights of words that
			// belong to this topic
			while (iterator.hasNext()) {
				IDSorter info = iterator.next();

				topicData.put(info.getID(), info.getWeight());
				totalWeight+= info.getWeight();
			}
			Iterator<Integer> hmIterator= topicData.keySet().iterator();
			// Normalizing the weights so that they can be used as probabilities
			while(hmIterator.hasNext()) {
				Integer id= hmIterator.next();
				topicData.put(id, topicData.get(id)/totalWeight);
			}

			phi.add((HashMap<Integer, Double>)topicData.clone());
		}

		// Printing out the words and probabilities
		for(int topic=0; topic < phi.size(); topic++) {
			HashMap<Integer, Double> topicData= phi.get(topic);
			Iterator<Integer> hmIterator= topicData.keySet().iterator();
			System.out.println(">>>>>>>> Topic: "+topic);
			while(hmIterator.hasNext()) {
				Integer id= hmIterator.next();
				System.out.println(" "+alphabet.lookupObject(id)+" "+topicData.get(id));
			}
		}

		// Extract the probabilities of topics per document
		int docLen;
		int[] topicCounts = new int[ topicModel.getNumTopics() ];
		ArrayList<TopicAssignment> data= topicModel.getData();
		IDSorter[] sortedTopics = new IDSorter[ topicModel.getNumTopics() ];
		for (int topic = 0; topic < topicModel.getNumTopics(); topic++) {
			// Initialize the sorters with dummy values
			sortedTopics[topic] = new IDSorter(topic, topic);
		}

		for (int doc = 0; doc < data.size(); doc++) {
			HashMap<Integer, Double> docData= new HashMap<Integer, Double>(0);
			LabelSequence topicSequence = (LabelSequence) data.get(doc).topicSequence;
			int[] currentDocTopics = topicSequence.getFeatures();



			if (data.get(doc).instance.getSource() != null) {
				System.out.println(data.get(doc).instance.getSource()); 
			}
			else {
				System.out.print ("null-source");
			}


			docLen = currentDocTopics.length;

			// Count up the tokens
			for (int token=0; token < docLen; token++) {
				topicCounts[ currentDocTopics[token] ]++;
			}

			// And normalize
			for (int topic = 0; topic < topicModel.getNumTopics(); topic++) {
				//sortedTopics[topic].set(topic, (float) topicCounts[topic] / docLen);
				docData.put(topic, (double) topicCounts[topic] / docLen);
			}

			Arrays.sort(sortedTopics);

			/*for (int i = 0; i < topicModel.getNumTopics(); i++) {
				System.out.print (sortedTopics[i].getID() + " " + 
						  sortedTopics[i].getWeight() + " ");
			}
			System.out.print (" \n");*/

			Arrays.fill(topicCounts, 0);
			theta.add((HashMap<Integer, Double>)docData.clone());

		}

		// Printing out the words and probabilities

		for(int doc=0; doc < theta.size(); doc++) {
			HashMap<Integer, Double> docData= theta.get(doc);
			Iterator<Integer> hmIterator= docData.keySet().iterator();
			double[] sortedDocs= new double[docData.keySet().size()];
			System.out.println(">>>>>>>> Doc: "+doc);
			while(hmIterator.hasNext()) {
				Integer id= hmIterator.next();
				sortedDocs[id]= docData.get(id);
				// System.out.println(" "+id+" "+docData.get(id));
			}
			for(int index=0; index < sortedDocs.length; index++){
				System.out.println(" "+ sortedDocs[index]);
			}

		}
		// Calculating the probabilities of generating the words in each document

		return logLikelihood;
	}
	/**
	 * This method using the combinations of all the LDA parameters and prints out the 
	 * likelihoods for all combinations
	 * @param alphasToBeConsidered
	 * @param useStopWordsList
	 * @param selectCriticalDocs
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public String selectOptimalParameters(InstanceList instances, String folderPath, double[] alphasToBeConsidered, double[] betasToBeConsidered,
			int[] iterationsToBeConsidered, int[] topicSizesToBeConsidered) throws IOException {
		
		// The comments read from the input comments file are saved as instances
		//InstanceList instances;
		// The LDA Topic Model
		ParallelTopicModel topicModel;
//		TextDocumentReader instanceGenerator= new TextDocumentReader();
		/*instanceGenerator.readCommentsFromDirectory(true,true,useStopWordsList,selectCriticalDocs);
		instances= instanceGenerator.getAllInstances();*/
		//instances= Utilities.readInstancesFromMalletFile(TopicConstants.outputFolderpath+"\\"+TopicConstants.allCommentsMalletFile);
		// Printing out Log Likelihoods for different alphas, betas, iterations and topic sizes
		CsvWriter likelihoodFile= new CsvWriter(folderPath+File.separator + "LikelihoodVsMalletParams.csv");
		String[] header= {"Alpha","Beta","#Itr","TopicSz","Likelihood"};
		likelihoodFile.writeRecord(header);
		int alphaIndex=0,betaIndex=0,iterationsIndex=0,topicSizeIndex=0;
		double optAlpha=0, optBeta=0;
		int optItr=0, optTopicSz=0;
		int optRow=0, count=0;
		double maxLikelihood=Double.NEGATIVE_INFINITY;
		for(alphaIndex=0; alphaIndex<alphasToBeConsidered.length; alphaIndex++){
			for(betaIndex=0; betaIndex<betasToBeConsidered.length; betaIndex++){
				for(iterationsIndex=0; iterationsIndex<iterationsToBeConsidered.length; iterationsIndex++){
					for(topicSizeIndex=0; topicSizeIndex<topicSizesToBeConsidered.length; topicSizeIndex++){
						buildTopicModelUsingLDA(instances, topicSizesToBeConsidered[topicSizeIndex], 
								topicSizesToBeConsidered[topicSizeIndex]*alphasToBeConsidered[alphaIndex], betasToBeConsidered[betaIndex], 
								iterationsToBeConsidered[iterationsIndex]);
						topicModel= getTopicModel();
						double currMaxlikelihood= topicModel.modelLogLikelihood();
						String[] record = {String.format("%f", alphasToBeConsidered[alphaIndex]), 
								String.format("%f", betasToBeConsidered[betaIndex]), 
								String.format("%d", iterationsToBeConsidered[iterationsIndex]), 
								String.format("%d", topicSizesToBeConsidered[topicSizeIndex]),
								String.format("%f", currMaxlikelihood)
						};
						likelihoodFile.writeRecord(record);
						likelihoodFile.flush();
						if (maxLikelihood < currMaxlikelihood) {
							maxLikelihood= currMaxlikelihood;
							optAlpha= alphasToBeConsidered[alphaIndex];
							optBeta= betasToBeConsidered[betaIndex];
							optItr= iterationsToBeConsidered[iterationsIndex];
							optTopicSz= topicSizesToBeConsidered[topicSizeIndex];
							optRow= count;
						}
						count++;
					}
				}
			}
		}
		likelihoodFile.close();
		String optRes= String.format("Alpha: %f, Beta: %f, Itr:%d, Topics:%d, Loglikelihood:%d, rownum: %d",(float)optAlpha, (float)optBeta, optItr, optTopicSz, (float)maxLikelihood, optRow);
		return optRes;
	}

	/**
	 * This method prints out the alphas values and the likelihoods for each of the alphas
	 * and we have to manually select the optimal alpha by looking at the file
	 * @param inputDir
	 * @param outputFilesDirectory
	 * @param alphasToBeConsidered
	 * @throws IOException
	 */
	public void selectOptimalAlpha(InstanceList instances, String inputDir, String outputFilesDirectory, double[] alphasToBeConsidered) throws IOException{
		PaperAbstractReader instanceGenerator= new PaperAbstractReader();

		instanceGenerator.readAbstractsFromDirectory(inputDir,true,true);
		instances= instanceGenerator.getAllInstances();

		// Printing out Log Likelihoods for different alphas
		double[] logLikelihoods= new double[alphasToBeConsidered.length];
		PrintWriter likelihoodFile= new PrintWriter(new File(outputFilesDirectory+"\\LikelihoodVsAlpha.txt"));
		likelihoodFile.println("Topic Size: "+TopicConstants.NUMTOPICS+" Beta: "+ TopicConstants.BETA+ "Sampling Iterations: "+TopicConstants.NUMITERATIONS);
		int alphaIndex=0;
		for(alphaIndex=0; alphaIndex<alphasToBeConsidered.length; alphaIndex++){
			buildTopicModelUsingLDA(instances, TopicConstants.NUMTOPICS, TopicConstants.NUMTOPICS*alphasToBeConsidered[alphaIndex], TopicConstants.BETA, TopicConstants.NUMITERATIONS);
			topicModel= getTopicModel();
			logLikelihoods[alphaIndex]= topicModel.modelLogLikelihood();

		}
		for(alphaIndex=0; alphaIndex<alphasToBeConsidered.length; alphaIndex++){
			likelihoodFile.println("Topic Size: "+alphasToBeConsidered[alphaIndex]+" Likelihood: "+logLikelihoods[alphaIndex]);
		}

		likelihoodFile.close();

	}

	/**
	 * This method prints out the beta values and the likelihoods for each of the betas
	 * and we have to manually select the optimal beta by looking at the file
	 * @param commentFilesDirectory
	 * @param outputFilesDirectory
	 * @param betasToBeConsidered
	 * @throws IOException
	 */
	public void selectOptimalBeta(InstanceList instances, String commentFilesDirectory, String outputFilesDirectory, double[] betasToBeConsidered) throws IOException{
		// The LDA Topic Model
		ParallelTopicModel topicModel;

		PaperAbstractReader instanceGenerator= new PaperAbstractReader();

		instanceGenerator.readAbstractsFromDirectory(commentFilesDirectory,true,true);
		instances= instanceGenerator.getAllInstances();

		// Printing out Log Likelihoods for different betas
		double[] logLikelihoods= new double[betasToBeConsidered.length];
		PrintWriter likelihoodFile= new PrintWriter(new File(outputFilesDirectory+"\\LikelihoodVsBeta.txt"));
		likelihoodFile.println("Topic Size: "+TopicConstants.NUMTOPICS+" Alpha: "+ TopicConstants.ALPHA+ "Sampling Iterations: "+TopicConstants.NUMITERATIONS);
		int betaIndex=0;
		for(betaIndex=0; betaIndex<betasToBeConsidered.length; betaIndex++){
			buildTopicModelUsingLDA(instances, TopicConstants.NUMTOPICS, TopicConstants.NUMTOPICS*TopicConstants.ALPHA, betasToBeConsidered[betaIndex], TopicConstants.NUMITERATIONS);
			topicModel= getTopicModel();
			logLikelihoods[betaIndex]= topicModel.modelLogLikelihood();

		}
		for(betaIndex=0; betaIndex<betasToBeConsidered.length; betaIndex++){
			likelihoodFile.println("Topic Size: "+betasToBeConsidered[betaIndex]+" Likelihood: "+logLikelihoods[betaIndex]);
		}

		likelihoodFile.close();

	}

	/**
	 * This method prints out the topic size values and the likelihoods for each of the topic sizes
	 * and we have to manually select the optimal topic size by looking at the file
	 * @param commentFilesDirectory
	 * @param outputFilesDirectory
	 * @param topicSizesToBeConsidered
	 * @throws IOException
	 */
	public void selectOptimalTopicSize(InstanceList instances, String commentFilesDirectory, String outputFilesDirectory, int[] topicSizesToBeConsidered) throws IOException{
		// The LDA Topic Model
		ParallelTopicModel topicModel;

		PaperAbstractReader instanceGenerator= new PaperAbstractReader();

		instanceGenerator.readAbstractsFromDirectory(commentFilesDirectory,true,true);
		instances= instanceGenerator.getAllInstances();

		// Printing out the Log Likelihoods for different topicSizes			

		double[] logLikelihoods= new double[topicSizesToBeConsidered.length];
		PrintWriter likelihoodFile= new PrintWriter(new File(outputFilesDirectory+"\\LikelihoodVsTopicSize.txt"));
		likelihoodFile.println("Alpha: "+TopicConstants.ALPHA+" Beta: "+ TopicConstants.BETA+ "Sampling Iterations: "+TopicConstants.NUMITERATIONS);
		int topicSizeIndex=0;
		for(topicSizeIndex=0; topicSizeIndex<topicSizesToBeConsidered.length; topicSizeIndex++){
			buildTopicModelUsingLDA(instances, topicSizesToBeConsidered[topicSizeIndex], topicSizesToBeConsidered[topicSizeIndex]*TopicConstants.ALPHA, TopicConstants.BETA, TopicConstants.NUMITERATIONS);
			topicModel= getTopicModel();
			logLikelihoods[topicSizeIndex]= topicModel.modelLogLikelihood();

		}

		for(topicSizeIndex=0; topicSizeIndex<topicSizesToBeConsidered.length; topicSizeIndex++){
			likelihoodFile.println("Topic Size: "+topicSizesToBeConsidered[topicSizeIndex]+" Likelihood: "+logLikelihoods[topicSizeIndex]);
		}

		likelihoodFile.close();

	}

	/**
	 * This method prints out the the number of iterations and the likelihoods for each of the iteration values
	 * and we have to manually select the optimal iterations by looking at the file
	 * @param commentFilesDirectory
	 * @param outputFilesDirectory
	 * @param iterationsToBeConsidered
	 * @throws IOException
	 */
	public void selectOptimalIterationsSize(InstanceList instances, String commentFilesDirectory, String outputFilesDirectory, int[] iterationsToBeConsidered) throws IOException{
		// The LDA Topic Model
		ParallelTopicModel topicModel;

		PaperAbstractReader instanceGenerator= new PaperAbstractReader();

		instanceGenerator.readAbstractsFromDirectory(commentFilesDirectory,true,true);
		instances= instanceGenerator.getAllInstances();

		// Printing out the Log Likelihoods for different iterations			

		double[] logLikelihoods= new double[iterationsToBeConsidered.length];
		PrintWriter likelihoodFile= new PrintWriter(new File(outputFilesDirectory+"\\LikelihoodVsIterations.txt"));
		likelihoodFile.println("Alpha: "+TopicConstants.ALPHA+" Beta: "+ TopicConstants.BETA+ "Topic Size: "+TopicConstants.NUMTOPICS);
		int iterationsIndex=0;
		for(iterationsIndex=0; iterationsIndex<iterationsToBeConsidered.length; iterationsIndex++){
			buildTopicModelUsingLDA(instances, TopicConstants.NUMTOPICS, TopicConstants.NUMTOPICS*TopicConstants.ALPHA, TopicConstants.BETA, iterationsToBeConsidered[iterationsIndex]);
			topicModel= getTopicModel();
			logLikelihoods[iterationsIndex]= topicModel.modelLogLikelihood();
			
		}
		for(iterationsIndex=0; iterationsIndex<iterationsToBeConsidered.length; iterationsIndex++){
			likelihoodFile.println("Iterations: "+iterationsToBeConsidered[iterationsIndex]+" Likelihood: "+logLikelihoods[iterationsIndex]);
		}
		
		likelihoodFile.close();

	}

}
