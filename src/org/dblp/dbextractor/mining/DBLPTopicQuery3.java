package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.TreeSet;

import org.dblp.dbextractor.DBLPVenue;
import org.dblp.dbextractor.mining.DBLPTopicQuery1.PaperDistribution;
import org.dblp.dbextractor.mining.DBLPTopicQuery2.TopicHalfLife;

import com.csvreader.CsvWriter;

public class DBLPTopicQuery3 extends DBLPQuery {
	class PaperYearCiteCount {
		int citeCount;
		int year;
		PaperYearCiteCount(int cnt, int y) {
			citeCount = cnt;
			year = y;
		}
	}
	class PaperHalfLife {
		int pubyr;
		int refyrcitecount;
		int citingHalfLife;
		float prospectiveHalfLife;
		PaperHalfLife(ArrayList<PaperYearCiteCount> count, int refYr, int pyr) {
			int totCount=0;
			int half= 0;
			int halfYr=0;
			prospectiveHalfLife= 0;
			pubyr=0;
			for (PaperYearCiteCount c : count) {
				totCount += c.citeCount;
				if (c.year== refYr && c.citeCount > 0) {
					refyrcitecount= c.citeCount;
					pubyr=pyr;
				}
			}
			half = (totCount+1) >> 1;
			totCount = 0;
			for (PaperYearCiteCount c : count) {
				if (totCount < half) 
					totCount += c.citeCount;
				else {
					halfYr = c.year;
					break;
				}
			}
			if (halfYr > 0) {
				prospectiveHalfLife= (float)(halfYr - pyr+1)/(float)(refYr - pyr +1);
			}
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
		outfile.write("topicid");
		outfile.write("top kws");
		int numsteps=0;
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
		getAllTopicPaperRelation(topicDistribution,DBLPVenue.getResearchDomainShortName(currResearchDomainId), conn, numsteps);
		System.out.println("Loaded " + topicDistribution.size() + " topics and topic-paper relations for the domain: " + 
				DBLPVenue.getResearchDomainShortName(currResearchDomainId));
		int stepcount=0;
		for (int curryr = earliestY; curryr <= latestY; curryr+= timestep) {
			int endyr= (curryr + timestep-1)>latestY?latestY:(curryr+timestep-1);
			String sql= getSQL1(DBLPVenue.getResearchDomainShortName(currResearchDomainId), endyr);
			Statement st;
			ResultSet rs;
			st= conn.createStatement();
			rs = st.executeQuery(sql);
			int paperid=0;
			int paperpubyr=0;
			ArrayList<PaperYearCiteCount> citationCount =new ArrayList<PaperYearCiteCount>();
			Hashtable<Integer,PaperHalfLife> paperHalfLife= new Hashtable<Integer,PaperHalfLife>();
			int count=0, entries=0;
			while (rs.next()) {
				if (rs.getInt(1)!= paperid && paperid != 0) {
					PaperHalfLife halfLife = new PaperHalfLife(citationCount, endyr, paperpubyr);
					citationCount=null; paperpubyr=0;
					citationCount=new ArrayList<PaperYearCiteCount>();
					if (paperHalfLife.get(paperid) == null )
						paperHalfLife.put(paperid, halfLife);
					else
						System.out.println("Error! Paper id " + paperid + " has duplicate entry");
				}
				paperid= rs.getInt(1);
				// I am forced to add this heuristics due to poor data quality. Sometimes citation year is 0. In that case I make citation year = publication yr + 1
				int citationyr= rs.getInt(3);
				paperpubyr= rs.getInt(4);
				if (citationyr == 0)
					citationyr = paperpubyr+1;
				if (citationyr < paperpubyr) 
					System.out.println("Error! Paper id " + paperid + " cited in " + citationyr + " but published in " + paperpubyr);
				else {
					citationCount.add(new PaperYearCiteCount(rs.getInt(2),citationyr));
					entries++;
				}
			}
			// The last one
			if (paperHalfLife.get(paperid) == null )
				paperHalfLife.put(paperid, new PaperHalfLife(citationCount, endyr, paperpubyr));
			else
				System.out.println("Error! Paper id " + paperid + " has duplicate entry");
			citationCount=null;
			System.out.println("Step: " + curryr + "-" + endyr + ": Total " + paperHalfLife.size() + " papers with prospective half-lives computed from " + entries + " entries" );
			rs.close();
			st.close();
			
			ArrayList<Float> prosHalfLives= new ArrayList<Float>();
			ArrayList<Integer> citedHalfLives= new ArrayList<Integer>();	
			entries=0;
			for (String topicid :  topicDistribution.keySet()) {
				TopicHalfLifeDistribution topicDist= topicDistribution.get(topicid);
				for (int papid : topicDist.paperids) {
					if (paperHalfLife.get(papid) == null )
						entries++;
					else {
						prosHalfLives.add(paperHalfLife.get(papid).prospectiveHalfLife);
						// We take only those papers which has > 0 citations on the reference year. 
						// Furthermore, we add the pubyr in the list as many times as the citation count of the reference year so as to bias the median value
						for (int i= 0; i < paperHalfLife.get(papid).refyrcitecount;i++)
							citedHalfLives.add(paperHalfLife.get(papid).pubyr);
					}
				}
				int citedHalfLife = (citedHalfLives.size()==0)?-1:computeMedian(citedHalfLives);
				float prospectiveHalfLife= (prosHalfLives.size()==0)?-1:computeMedian(prosHalfLives); 
				if (citedHalfLife != -1)
					citedHalfLife= (endyr - citedHalfLife)+1; 
				topicDist.citedHalfLives[stepcount]= citedHalfLife;
				topicDist.prospectiveHalfLives[stepcount]= prospectiveHalfLife; 
			}
			if (entries > 0)
				System.out.println("Error! " + entries + " papers do not have Half-life in step " + curryr + "-" + endyr);
			stepcount++;
		}
		
		// Now dumping the result to the csv file
		System.out.println("Creating csv file...");
		String[] record = new String[recordSize];
		TreeSet<String> sortedTopics = new TreeSet<String>(topicDistribution.keySet());
		for (String tid : sortedTopics) {
			TopicHalfLifeDistribution topicDist= topicDistribution.get(tid);
			record[0]=tid; record[1]= topicDist.topkws;
			for (stepcount=0; stepcount < numsteps; stepcount++) {
				record[stepcount+2]= String.valueOf(topicDist.citedHalfLives[stepcount]);
				record[numsteps+stepcount+2]= String.valueOf(topicDist.prospectiveHalfLives[stepcount]);
			}
			outfile.writeRecord(record);
		}
	}
	private String getSQL1(String shortname, int yr) {
		String papersRecvdCitation = "SELECT c.id as id, count(c.id) as citecount, c.citation_year, p.year FROM dblp_citation_" + shortname +
		" c, dblp_pub_"+ shortname +" p WHERE c.citation_year <= " + yr + " AND p.year <= " + yr + " AND p.id = c.id GROUP BY c.id, c.citation_year ORDER BY c.id, c.citation_year" ;
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
