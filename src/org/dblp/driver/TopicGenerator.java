package org.dblp.driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.dblp.dbextractor.DBLPVenue;
import org.dblp.dbextractor.DataExporterImporter;
import org.dblp.dbextractor.DataManager;
import org.dblp.dbextractor.mining.DBLPTopicAuthCollabQuery;
import org.dblp.dbextractor.mining.DBLPTopicQuery2;
import org.dblp.dbextractor.mining.DBLPTopicQuery4;
import org.dblp.dbextractor.mining.DBLPTopicQuery5;
import org.dblp.topicmining.PaperAbstractReader;
import org.dblp.topicmining.TopicConstants;
import org.dblp.topicmining.TopicTrainingModel;
import org.dblp.topicmining.Utilities;

import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.types.InstanceList;

import com.csvreader.CsvWriter;


public class TopicGenerator {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws Exception {

		try {
			DataManager mgr = new DataManager();
			mgr.connectmySql("dblp");
//			int [] counts= createMalletInstances(mgr, DBLPVenue.SE);
//			optimizeLDAParameters();
//			generateTopicFromAbstractsInDB();
//			importTopicsToDatabase(mgr, DBLPVenue.SE);
//			System.out.println("Total " + counts[0] + " papers with " + (counts[0]-counts[1]) + 
//					" abstracts have been used for topic generation");

			runAllTopicAnalysisQueries(mgr, DBLPVenue.SE);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void generateTopicFromAbstractsInDB()
			throws SQLException, FileNotFoundException, IOException,
			ClassNotFoundException {
		InstanceList allInstances, trainingInstances= null, testingInstances= null;
		ParallelTopicModel topicModel;
		TopicTrainingModel ldaTrainModel= new TopicTrainingModel();
		
		allInstances= Utilities.readInstancesFromMalletFile(TopicConstants.INPUTDIRPATH+
				File.separator+TopicConstants.ALLMALLETFILENAME);
		trainingInstances= Utilities.readInstancesFromMalletFile(TopicConstants.INPUTDIRPATH+
				File.separator+TopicConstants.TRAININGMALLETFILENAME);
		testingInstances= Utilities.readInstancesFromMalletFile(TopicConstants.INPUTDIRPATH+
				File.separator+TopicConstants.TESTINGMALLETFILENAME);
		/********** Extraction of topics from training data using the optimal parameters ************/
		ldaTrainModel.buildTopicModelUsingLDA(trainingInstances, TopicConstants.NUMTOPICS, TopicConstants.NUMTOPICS*TopicConstants.ALPHA, TopicConstants.BETA, TopicConstants.NUMITERATIONS);
		topicModel= ldaTrainModel.getTopicModel();
		ldaTrainModel.generateTopicOutputFiles(topicModel,TopicConstants.OUTPUTDIRPATH);
	}
	private static int[] createMalletInstances(DataManager mgr, int venueId)
			throws SQLException, FileNotFoundException, IOException {
		PaperAbstractReader instanceGenerator= new PaperAbstractReader();
		instanceGenerator.setDomainId(venueId);
		String inputPath= TopicConstants.INPUTDIRPATH + File.separator + "papers";
		//	instanceGenerator.readAbstractsFromDirectory(MalletConstants.commentsFolderPath,true,true);
		int [] counts= instanceGenerator.readAbstractsFromDatabase(mgr, inputPath, true, true);
		instanceGenerator.splitInstances(0.8, 0.0, TopicConstants.INPUTDIRPATH);
		Utilities.writeInstancesToMalletFile(instanceGenerator.getAllInstances(), TopicConstants.INPUTDIRPATH+
				File.separator +TopicConstants.ALLMALLETFILENAME);
		Utilities.writeInstancesToMalletFile(instanceGenerator.getTrainingInstances(), TopicConstants.INPUTDIRPATH+
				File.separator+TopicConstants.TRAININGMALLETFILENAME);
		Utilities.writeInstancesToMalletFile(instanceGenerator.getTestingInstances(), TopicConstants.INPUTDIRPATH+
				File.separator+TopicConstants.TESTINGMALLETFILENAME);
		return counts;
	}
	
	private static void importTopicsToDatabase(DataManager d, int venueId) throws SQLException, IOException {
		Connection conn = d.getConnection();
		int[] years= d.getMinMaxYearForResearchDomain(venueId);
		DataExporterImporter importer = new DataExporterImporter();
		String topicKWFile = TopicConstants.OUTPUTDIRPATH+File.separator + TopicConstants.TOPICKWFILENAME;
		String docTopicFile = TopicConstants.OUTPUTDIRPATH+File.separator + TopicConstants.TOPICDOCFILENAME;
		importer.importTopics(conn, venueId, years[0], years[1], docTopicFile, topicKWFile);
	}

	/** Best values for SE: 80 topics, 20000 iterations, alpha= 0.001, beta= 0.01 */
	private static void optimizeLDAParameters() throws IOException, ClassNotFoundException {
		/********** Selection of optimal Parameters ***********/
//		double[] alphasToBeConsidered= {0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1};
//		double[] betasToBeConsidered= {0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1};
//		int[] topicSizesToBeConsidered= {3, 5, 10, 15, 20, 30, 40, 50, 60, 80, 100};
//		int[] iterationsToBeConsidered= {5, 10, 50, 100, 500, 1000, 2000, 2500, 3000};
		double[] alphasToBeConsidered= {0.001};
		double[] betasToBeConsidered= {0.01};
		int[] topicSizesToBeConsidered= {90};
		int[] iterationsToBeConsidered= {20000};

		InstanceList trainingInstances= Utilities.readInstancesFromMalletFile(TopicConstants.INPUTDIRPATH+
				File.separator+TopicConstants.ALLMALLETFILENAME);
		TopicTrainingModel ldaTrainModel= new TopicTrainingModel();
		String res= ldaTrainModel.selectOptimalParameters(trainingInstances, TopicConstants.OUTPUTDIRPATH, alphasToBeConsidered, betasToBeConsidered, 
				iterationsToBeConsidered, topicSizesToBeConsidered);
		System.out.println(res);
/*		ldaTrainModel.selectOptimalAlpha(trainingInstances, inputDir,outputDir, alphasToBeConsidered);
		ldaTrainModel.selectOptimalBeta(trainingInstances, inputDir, outputDir, betasToBeConsidered);
		ldaTrainModel.selectOptimalIterationsSize(trainingInstances, inputDir, outputDir, iterationsToBeConsidered);
		ldaTrainModel.selectOptimalTopicSize(trainingInstances, inputDir, outputDir, topicSizesToBeConsidered); */
	}
	private static void runAllTopicAnalysisQueries(DataManager mgr, int venueId) throws Exception {
		int[] stend= mgr.getMinMaxYearForResearchDomain(venueId);
		int timestep = 1;
		runTopicDistribution(mgr, venueId, stend, timestep);
//		runTopicCollab(mgr, venueId, stend);

	}
	private static void runTopicCollab(DataManager mgr, int venueId, int[] stend)
			throws SQLException, IOException {
		CsvWriter outfile;
		DBLPTopicAuthCollabQuery tquery2 = new DBLPTopicAuthCollabQuery();
		tquery2.setResearchDomain(venueId);
		tquery2.setEarliestLatestYear(stend[0], stend[1]);
		tquery2.setTopicBasedCollaboration();
		outfile= new CsvWriter("out/topiccollabindex.csv");
		tquery2.processQuery(mgr.getConnection(), outfile);
		outfile.close();
		tquery2.setKWBasedCollaboration();
		outfile= new CsvWriter("out/collabindex.csv");
		tquery2.processQuery(mgr.getConnection(), outfile);
		outfile.close();
	}
	private static void runTopicDistribution(DataManager mgr, int venueId,
			int[] stend, int timestep) throws SQLException, IOException {
		CsvWriter outfile;
/*		outfile= new CsvWriter("out/topicq1_total.csv");
		DBLPTopicQuery1 tquery1 = new DBLPTopicQuery1();
		tquery1.setResearchDomain(venueId);
		tquery1.setEarliestLatestYear(stend[0], stend[1]);
		tquery1.setTimeStep(timestep);
		System.out.println("Processing Topic Query#1- by Paper Count");
	
		tquery1.setTopicDistributionByPaperCount();
		tquery1.processQuery(mgr.getConnection(), outfile);
		outfile.close();
		System.out.println("Processing Topic Query#1- by Citation Count Hindsight");
		
		tquery1.setTopicDistributionByCitationHindsight();
		outfile= new CsvWriter("out/topicq1cite_hindsight_total.csv");
		tquery1.processQuery(mgr.getConnection(), outfile);
		outfile.close();
		
		System.out.println("Processing Topic Query#1- by Citation Count Yearbased");
		// Below two lines are useless. Doing it since the data import format was wrong 
		DataExporterImporter importer = new DataExporterImporter();
		importer.createCitationTable(mgr.getConnection(), venueId);
		
		tquery1.setTopicDistributionByCitationInAYear();
		outfile= new CsvWriter("out/topicq1cite_total.csv");
		tquery1.processQuery(mgr.getConnection(), outfile);
		outfile.close();
		*/
/*	
		DBLPTopicAffinityQuery tquery2= new DBLPTopicAffinityQuery();
		tquery2.setResearchDomain(venueId);
		tquery2.setEarliestLatestYear(stend[0], stend[1]);
		tquery2.setBeginEndYear(stend[0], stend[1]);
		outfile= new CsvWriter("out/author_topicaffinity.csv");
		tquery2.processQuery(mgr.getConnection(), outfile);
		outfile.close();
*/
		/*
		DBLPTopicAffinityQuery tquery2= new DBLPTopicAffinityQuery();
		tquery2.setResearchDomain(venueId);	
		outfile= new CsvWriter("out/author_topicaffinity_"+stend[0] + "-" + stend[0]+".csv");
		tquery2.setBeginEndYear(stend[0], stend[0]);
		System.out.println("Processing Query to generate Topic Affinity for each author for " + stend[0] + "-" + stend[0]);
		tquery2.processQuery(mgr.getConnection(), outfile);
		outfile.close();

		for (int curryr = stend[0]+timestep; curryr <= stend[1]; curryr+=timestep) {
			int endyr= (curryr + timestep-1)>stend[1]?stend[1]:(curryr+timestep-1);
			String yrRange = stend[0] + "-" + endyr;
			outfile= new CsvWriter("out/author_topicaffinity_"+yrRange+".csv");
			tquery2.setBeginEndYear(stend[0], endyr);
			System.out.println("Processing Query to generate Topic Affinity for each author for " + yrRange);
			tquery2.processQuery(mgr.getConnection(), outfile);
			outfile.close();
		}
*/
		System.out.println("Processing Topic Query#4- half-life calculation based on Cited Half-Life and prospective half-life");
		outfile= new CsvWriter("out/newtopichl.5.csv");

/*		DBLPTopicQuery2 tquery2 = new DBLPTopicQuery2();
		tquery2.setResearchDomain(venueId);
		tquery2.setEarliestLatestYear(stend[1], stend[1]);
		tquery2.setTimeStep(timestep);
		tquery2.processQuery(mgr.getConnection(), outfile);
		outfile.close(); */

		DBLPTopicQuery5 tquery4 = new DBLPTopicQuery5();
		tquery4.setResearchDomain(venueId);
		tquery4.setEarliestLatestYear(stend[0], stend[1]);
		tquery4.setTimeStep(timestep);
		tquery4.processQuery(mgr.getConnection(), outfile);
		outfile.close();
	}

}
