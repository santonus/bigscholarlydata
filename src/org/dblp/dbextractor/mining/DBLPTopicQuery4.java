package org.dblp.dbextractor.mining;
/**
 * This is implemented to fix the bug of DBLPQuery3
 */
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.TreeSet;

import org.dblp.dbextractor.DBLPVenue;
import org.dblp.dbextractor.mining.DBLPTopicQuery3.PaperHalfLife;
import org.dblp.dbextractor.mining.DBLPTopicQuery3.PaperYearCiteCount;
import org.dblp.dbextractor.mining.DBLPTopicQuery3.TopicHalfLifeDistribution;

import com.csvreader.CsvWriter;

public class DBLPTopicQuery4 extends DBLPQuery {
	class PaperHalfLife {
		int pubyr;
		Hashtable<Integer,Integer> count;
		private TreeSet<Integer> sortedYears;
		PaperHalfLife(Hashtable<Integer,Integer> c, int pyr) {
			count=c;
			pubyr=pyr;
			sortedYears = new TreeSet<Integer>(count.keySet());
		}
		final int computeRefYrCitationCount(int refyr) {
			if (count.get(refyr)==null)
				return 0;
			else
				return count.get(refyr);
		}
		final int getHalfyrFromRefYr(int refyr) {
			int totCount=0;
			int half= 0;
			int halfYr=0;
			for (int yr : sortedYears) {
				if (yr <= refyr) {
					totCount += count.get(yr);
				} else
					break;
			}
			half = (totCount+1) >> 1;
				totCount = 0;
				for (int yr : sortedYears) {
					if (totCount < half) 
						totCount += count.get(yr);
					else {
						halfYr = yr;
						break;
					}
				}
				return halfYr;
		}
	}
	class TopicHalfLifeDistribution {
		String topkws;
		ArrayList<Integer> paperids;
		int[] citedHalfLives;
		float[] prospectiveHalfLives;
		TopicHalfLifeDistribution(String kw, int n) {
			topkws = kw;
			citedHalfLives = new int[n];
			prospectiveHalfLives = new float[n];
			paperids = new ArrayList<Integer>();
		}
		/*		@Override
		public int compareTo(Object arg0) {
			PaperDistribution pd = (PaperDistribution) arg0;
			return topicId.compareToIgnoreCase(pd.topicId);
		}*/
	}

	@Override
	public void processQuery(Connection conn, CsvWriter outfile)
	throws SQLException, IOException {
		int numsteps=0;
		outfile.write("topicid");
		outfile.write("top kws");

		for (int curryr = earliestY; curryr <= latestY; curryr+= timestep) {
			int endyr= (curryr + timestep-1)>latestY?latestY:(curryr+timestep-1);
			String yrRange = "C_"+ curryr + "-" + endyr;
			outfile.write(yrRange);
			numsteps++;
		}

		for (int curryr = earliestY; curryr <= latestY; curryr+= timestep) {
			int endyr= (curryr + timestep-1)>latestY?latestY:(curryr+timestep-1);
			String yrRange = "P_"+ curryr + "-" + endyr;
			outfile.write(yrRange);
		}
		outfile.endRecord();
		int recordSize = numsteps*2 + 2;
		Hashtable<String,TopicHalfLifeDistribution> topicDistribution= new Hashtable<String,TopicHalfLifeDistribution>();
		getAllTopicPaperRelation(topicDistribution, DBLPVenue.getResearchDomainShortName(currResearchDomainId), conn, numsteps);

		System.out.println("Loaded " + topicDistribution.size() + " topics and topic-paper relations for the domain: " + 
				DBLPVenue.getResearchDomainShortName(currResearchDomainId));

		Hashtable<Integer,PaperHalfLife> papers = new Hashtable<Integer,PaperHalfLife>();
		getAllPaperCitation(papers, DBLPVenue.getResearchDomainShortName(currResearchDomainId),  conn);

		System.out.println("Creating csv file...");
		String[] record = new String[recordSize];
		TreeSet<String> sortedTopics = new TreeSet<String>(topicDistribution.keySet());
		for (String topicid :  sortedTopics) { 
			int stepcount=0;
			record[0]=topicid; record[1]= topicDistribution.get(topicid).topkws;
			for (int curryr = earliestY; curryr <= latestY; curryr+= timestep) {
				int endyr= (curryr + timestep-1)>latestY?latestY:(curryr+timestep-1);	
				ArrayList<Integer> citedHalfLives= new ArrayList<Integer>();
				ArrayList<Integer> prosHalfLives= new ArrayList<Integer>();
				ComputeHalfLivesForATopic(topicDistribution, papers, endyr, topicid, citedHalfLives, prosHalfLives);

				int citedHalfLife = (citedHalfLives.size()==0)?-1:computeMedian(citedHalfLives);
				if (citedHalfLife != -1)
					citedHalfLife= (endyr - citedHalfLife)+1; 
				int prospectiveHalfLife= (prosHalfLives.size()==0)?-1:computeMedian(prosHalfLives); 
				record[stepcount+2]= String.valueOf(citedHalfLife);
				record[numsteps+stepcount+2]= String.valueOf(prospectiveHalfLife);
				citedHalfLives=null; prosHalfLives= null; // To free the memory
				stepcount++;
			}
			outfile.writeRecord(record);
		}
	}
	private void ComputeHalfLivesForATopic(
			Hashtable<String, TopicHalfLifeDistribution> topicDistribution,
			Hashtable<Integer, PaperHalfLife> papers, int endyr,
			String topicid, ArrayList<Integer> citedHalfLives,
			ArrayList<Integer> prosHalfLives) {
		TopicHalfLifeDistribution aTopic= topicDistribution.get(topicid);
		
		for (int paperid: aTopic.paperids) {
			PaperHalfLife ph= papers.get(paperid);
			if (ph != null) {
				int citecount= ph.computeRefYrCitationCount(endyr);
				int halfYr= ph.getHalfyrFromRefYr(endyr);
				if (halfYr > 0) {
					int halfYrCount = halfYr - ph.pubyr + 1;
					prosHalfLives.add(halfYrCount);
				}
				for (int i=0; i < citecount; i++) {
					citedHalfLives.add(ph.pubyr);
				}
			}	
		}
	}
	private String getSQL1(String shortname) {
		String papersRecvdCitation = "SELECT c.id as id, count(c.id) as citecount, c.citation_year, p.year FROM dblp_citation_" + shortname +
		" c, dblp_pub_"+ shortname +" p WHERE p.id = c.id GROUP BY c.id, c.citation_year ORDER BY c.id, c.citation_year" ;
		return papersRecvdCitation;
	}
	private String getSQL2(String shortname) {
		String topicSQL = "SELECT tid, id FROM dblp_doctopic_" + shortname + " ORDER BY tid"; 
		return topicSQL;
	}
	private <T extends Comparable<? super T>> T computeMedian(List<T> list) {
		Collections.sort(list);
		int len = list.size();
		int median= 0;
		if ((len%2)==0) {
			median = len/2;
		} else
			median = (len+1)/2;
		return list.get(median-1);
	}
	private void getAllPaperCitation(Hashtable<Integer,PaperHalfLife> papers, String shortname, Connection conn) throws SQLException {
		String sql= getSQL1(shortname);
		Statement st= conn.createStatement();
		ResultSet rs = st.executeQuery(sql);
		StringBuffer topkws=new StringBuffer();
		int goodentries=0, badentries=0;
		int paperid=0, paperpubyr=0, citationcount=0, citationyr=0;
		Hashtable<Integer,Integer> papercitationcount=new Hashtable<Integer,Integer>();
		while (rs.next()) {
			if (rs.getInt(1)!= paperid && paperid != 0) {	
				if (papers.get(paperid) == null ) {
					papers.put(paperid, new PaperHalfLife(papercitationcount, paperpubyr));
				}
				else
					System.out.println("Error! Paper id " + paperid + " has duplicate entry");
				papercitationcount=null; paperpubyr=0;
				papercitationcount= new Hashtable<Integer,Integer>();
			}
			paperid= rs.getInt(1);
			citationcount= rs.getInt(2);
			citationyr= rs.getInt(3);
			paperpubyr= rs.getInt(4);

			// I am forced to add this heuristics due to poor data quality. Sometimes citation year is 0. In that case I make citation year = publication yr + 1. Sometimes citation yr > pubyr!!	
			if (citationyr == 0)
				citationyr = paperpubyr+1;
			if (citationyr < paperpubyr) {
				System.out.println("Error! Paper id " + paperid + " cited in " + citationyr + " but published in " + paperpubyr);
				badentries++;
			}
			else {
				papercitationcount.put(citationyr, citationcount);
				goodentries++;
			}
		}
		// Last one
		if (papers.get(paperid) == null ) {
			papers.put(paperid, new PaperHalfLife(papercitationcount, paperpubyr));
		}
		else
			System.out.println("Error! Paper id " + paperid + " has duplicate entry");
		papercitationcount=null;
		System.out.println(" Total " + papers.size() + " papers with citations retrieved. There are: " + badentries + " BAD entries" );
		rs.close();
		st.close();
	}
	private void getAllTopicPaperRelation(Hashtable<String,TopicHalfLifeDistribution> topicDistribution, String shortname, Connection conn, int numsteps) throws SQLException {
		String sql= "SELECT t.tid, t.keyword FROM dblp_topic_"+shortname+ " AS t WHERE t.istop = 1 order by t.tid";
		Statement st= conn.createStatement();
		ResultSet rs = st.executeQuery(sql);
		StringBuffer topkws=new StringBuffer();
		int papercount=0;
		String topicid="";
		while (rs.next()) {
			if (!rs.getString(1).equals(topicid) && !topicid.equals("")) {
				TopicHalfLifeDistribution paperDist= topicDistribution.get(topicid);
				topkws.trimToSize();
				if (paperDist==null ) {
					paperDist= new TopicHalfLifeDistribution(topkws.toString(), numsteps);
					topicDistribution.put(topicid, paperDist);
				}
				topkws=null;
				topkws=new StringBuffer();
			}
			topicid= rs.getString(1);
			topkws.append(rs.getString(2) + " ");
		}
		rs.close();
		st.close();
		// Last Topic
		TopicHalfLifeDistribution paperDist= topicDistribution.get(topicid);
		topkws.trimToSize();
		if (paperDist==null ) {
			paperDist= new TopicHalfLifeDistribution(topkws.toString(), numsteps);
			topicDistribution.put(topicid, paperDist);
		}
		// Now we put Topic and Paper relations
		sql= getSQL2(shortname);
		st= conn.createStatement();
		rs = st.executeQuery(sql);

		while (rs.next()) {
			topicid= rs.getString(1);
			int paperid = rs.getInt(2);
			TopicHalfLifeDistribution topicDist= topicDistribution.get(topicid);
			if (topicDist == null )
				System.out.println("Data discrepancy: topic id " + topicid + " has not been retrieved earlier");
			else {
				topicDist.paperids.add(paperid);
			}
		}
		rs.close();
		st.close();
	}
}
