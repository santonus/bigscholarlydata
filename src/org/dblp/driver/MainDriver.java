package org.dblp.driver;

import java.io.File;

import org.dblp.dbextractor.DBLPSanityTest;
import org.dblp.dbextractor.DBLPVenue;
import org.dblp.dbextractor.DataManager;
import org.dblp.dbextractor.mining.DBLPQuery0;
import org.dblp.dbextractor.mining.DBLPRQ5Query;

import com.csvreader.CsvWriter;

public class MainDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		DataManager mgr = new DataManager();
		String appBase= "out" + File.separator + "bib";
		mgr.setAppBase(appBase);
		//mgr.connect("bibdb");
		mgr.connectmySql("dblp");
		collectMetadata(mgr,DBLPVenue.SE);
		collectMetadata(mgr,DBLPVenue.OS);
		collectMetadata(mgr,DBLPVenue.DB);
		collectMetadata(mgr,DBLPVenue.IR);
		collectMetadata(mgr,DBLPVenue.AI);
		/*
		System.out.println("Starting Sanity Test");
		doAllSanityTest(mgr);
		*/

		//runAllQueries(mgr);

		
		// Delimiter is changed first. Hypen is not a delim here
/*		TextStringDocument.tokenizerDelim = " \t\n\r\f\'\"\\1234567890!@#$%^&*()_+={}|[]:;<,>.?/`~"; 
		int count= mgr.createTextCorpusFromDB(false);
		LDAPreprocessing ldaDriver = new LDAPreprocessing();
		ldaDriver.setAppBase(appBase);
		TestSequenceFilesFromDirectory conv = new TestSequenceFilesFromDirectory();
//		conv.createSeqFile(appBase + File.separator + "corpus", 
//				appBase + File.separator + "seq");
//		ldaDriver.setSeqFileDir("seq");
		ldaDriver.setSparseVectorDir("sparse");
		ldaDriver.setLDAOutputDir("lda");
		ldaDriver.setTopicOutputDir("result");
//		ldaDriver.runMahout();	*/	
	}
	
	private static void doAllSanityTest(DataManager mgr) throws Exception {
		DBLPSanityTest.testDBLPDump(mgr.getConnection());
		mgr.setupMysqlDB(DBLPVenue.IR);
		DBLPSanityTest.testVenueDetails(mgr.getConnection(),DBLPVenue.IR);
		/*		
		mgr.setupMysqlDB(DBLPVenue.AI);
		DBLPSanityTest.testVenueDetails(mgr.getConnection(),DBLPVenue.AI);
		mgr.setupMysqlDB(DBLPVenue.OS);
		mgr.setupMysqlDB(DBLPVenue.DB);
		
		
		DBLPSanityTest.testVenueDetails(mgr.getConnection(),DBLPVenue.OS);
		DBLPSanityTest.testVenueDetails(mgr.getConnection(),DBLPVenue.DB);
		DBLPSanityTest.testVenueDetails(mgr.getConnection(),DBLPVenue.SE);
		
		*/
	}
	
	private static void collectMetadata (DataManager mgr, int id) throws Exception {
		int[] stend= mgr.getMinMaxYearForResearchDomain(id);
		DBLPQuery0 q0 = new DBLPQuery0();
		q0.setResearchDomain(id);
		CsvWriter outfile= new CsvWriter("out/Metadata-"+DBLPVenue.getResearchDomainShortName(id)+".csv");
		System.out.println("Processing Query#0 for "+DBLPVenue.getResearchDomainShortName(id));
		q0.processQuery(mgr.getConnection(), outfile);
		outfile.close();
	}
	
	private static void runAllQueries(DataManager mgr) throws Exception {
		int[] stend= mgr.getMinMaxYearForResearchDomain(DBLPVenue.SE);
		int publicationCutOff=3, timestep=3;
		double nonCollaboratingRelationGenerationPercentage = 0.7;
		CsvWriter outfile;
		
		DBLPRQ5Query rq5q = new DBLPRQ5Query();
		rq5q.setResearchDomain(DBLPVenue.SE);
		rq5q.setEarliestLatestYear(stend[0], stend[1]);
		outfile= new CsvWriter("out/rq5query.csv");
		System.out.println("Processing Query#RQ5");
		rq5q.processQuery(mgr.getConnection(), outfile);
		outfile.close();
		
		/*DBLPQuery0 q0 = new DBLPQuery0();
		q0.setResearchDomain(DBLPVenue.SE);
		outfile= new CsvWriter("out/query0.csv");
		System.out.println("Processing Query#0");
		q0.processQuery(mgr.getConnection(), outfile);
		outfile.close();
					
		DBLPQuery1 q1= new DBLPQuery1(publicationCutOff, timestep);
		q1.setResearchDomain(DBLPVenue.DB);
		q1.setEarliestLatestYear(stend[0], stend[1]);
		outfile= new CsvWriter("out/query1.csv");
		System.out.println("Processing Query#1");
		q1.processQuery(mgr.getConnection(), outfile);
		outfile.close();
		
		DBLPQuery4 q4 = new DBLPQuery4();
		q4.setResearchDomain(DBLPVenue.DB);
		q4.setEarliestLatestYear(stend[0], stend[1]);
		outfile= new CsvWriter("out/query4.csv");
		System.out.println("Processing Query#4");
		q4.processQuery(mgr.getConnection(), outfile);
		outfile.close();
		
		DBLPQuery28 q28 = new DBLPQuery28();
		q28.setResearchDomain(DBLPVenue.DB);
		q28.setEarliestLatestYear(stend[0], stend[1]);
		q28.setNoCollabGenerationPercentage(nonCollaboratingRelationGenerationPercentage);
		*/
/*		for (int curryr = stend[0]+timestep; curryr <= stend[1]; curryr+=timestep) {
			int endyr= (curryr + timestep-1)>stend[1]?stend[1]:(curryr+timestep-1);
			String yrRange = curryr + "-" + endyr;
			outfile= new CsvWriter("out/q28_"+yrRange+".csv");
			q28.setBeginEndYear(curryr, endyr);
			System.out.println("Processing Query#28 for " + yrRange);
			q28.processQuery(mgr.getConnection(), outfile);
			outfile.close();
		} */
		/*
		q28.setBeginEndYear(1971, 1973);
		outfile= new CsvWriter("out/q28_1971-1973.csv");
		System.out.println("Processing Query#28");
		q28.processQuery(mgr.getConnection(), outfile);
		outfile.close();

		q28.setBeginEndYear(1974, 1975);
		outfile= new CsvWriter("out/q28_1974-1975.csv");
		System.out.println("Processing Query#28");
		q28.processQuery(mgr.getConnection(), outfile);
		outfile.close();

		q28.setBeginEndYear(1976, 1979);
		outfile= new CsvWriter("out/q28_1976-1979.csv");
		System.out.println("Processing Query#28");
		q28.processQuery(mgr.getConnection(), outfile);
		outfile.close();
			
		q28.setBeginEndYear(1980, 1982);
		outfile= new CsvWriter("out/q28_1980-1982.csv");
		System.out.println("Processing Query#28");
		q28.processQuery(mgr.getConnection(), outfile);
		outfile.close();
		
		q28.setBeginEndYear(1983, 1985);
		outfile= new CsvWriter("out/q28_1983-1985.csv");
		System.out.println("Processing Query#28");
		q28.processQuery(mgr.getConnection(), outfile);
		outfile.close();

		q28.setBeginEndYear(1986, 1988);
		outfile= new CsvWriter("out/q28_1986-1988.csv");
		System.out.println("Processing Query#28");
		q28.processQuery(mgr.getConnection(), outfile);
		outfile.close();

		q28.setBeginEndYear(1989, 1991);
		outfile= new CsvWriter("out/q28_1989-1991.csv");
		System.out.println("Processing Query#28");
		q28.processQuery(mgr.getConnection(), outfile);
		outfile.close();
		
		q28.setBeginEndYear(1992, 1994);
		outfile= new CsvWriter("out/q28_1992-1994.csv");
		System.out.println("Processing Query#28");
		q28.processQuery(mgr.getConnection(), outfile);
		outfile.close();

		q28.setBeginEndYear(1995, 1997);
		outfile= new CsvWriter("out/q28_1995-1997.csv");
		System.out.println("Processing Query#28");
		q28.processQuery(mgr.getConnection(), outfile);
		outfile.close();

		q28.setBeginEndYear(1998, 2000);
		outfile= new CsvWriter("out/q28_1998-2000.csv");
		System.out.println("Processing Query#28");
		q28.processQuery(mgr.getConnection(), outfile);
		outfile.close();
*/
		
	}
}
