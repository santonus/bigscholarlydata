package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.dblp.dbextractor.DBLPVenue;

import com.csvreader.CsvWriter;
/**
 * This query computes half-life based on citation, as per CACM 2010 Sjoberg's definition of 
 * journal half-life.
 * @author santonu
 * Citation Half-Life metrics
Here we are looking at three measures that have appeared in Sjoberg’s CACM article that 
Reviewer 4 mentioned. Two of the metrics are used for measuring half-lives of journals (Cited half-life 
and citing half-life) and one for articles (prospective half-life). The journal ones can easily apply 
for topics, since one can imagine a topic as a journal that publishes papers only in that topic. Mapping an 
article-based measure to topics may be problematic, because an article once published does not change, 
whereas a topic is dynamic in that new articles of varying quality are added over time. 

 * Cited Half-Life
Cited half-life is measured by using a year (for example, the current year) as a reference point. 
For example, the ISI Web of Knowledge publishes Journal Citation Reports (JCRs) annually, and uses the 
JCR year as the reference. The cited half-life of a journal is defined as: 

“The median age of the articles [published in the journal] that were cited [in ‘all’ avenues] in the 
JCR year. Half of a journal's cited articles were published more recently than the cited half-life. 
For example, in JCR 2001 the journal Crystal Research and Technology has a cited half-life of 7.0. 
That means that articles published in Crystal Research and Technology between 1995-2001 (inclusive) 
account for 50% of all citations to articles from that journal in 2001.”  

We could replace ‘journal’ by ‘topic’, and derive a definition for cited half-life of a topic as the 
median age of the papers belonging to the topic that received citations in a year of interest. The year 
of interest for us could be 2010. The current spreadsheet does not have the data to find cited half-life.

Following is a method to calculate the median age of the papers published in a topic that were cited in 
the reference year:

	1.	Choose a reference year, y (for example, 2010).
	2.	For each topic, t
		a.	Find the list, Ld of all papers in that topic that have received one or more citations in year y
		b.	Find the set Yd of the year of publication of each paper in Ld 
		c.	Find md, the median of the years in Yd
		d.	Cited half-life of topic, t is  (y – md + 1)

Citing half-life
The ISI Web of Knowledge gives the following definition: 
“The citing half-life is the median age of articles cited by the journal in the JCR year. 
For example, in JCR 2003, the journal Food Biotechnology has a citing half-life of 9.0. That means that 
50% of all articles cited by articles in Food Biotechnology in 2003 were published between 1995 and 2003 
(inclusive).” 

We can derive a definition for citing half-life of a topic as the median age of the articles cited by 
the papers published in that topic in a year of interest (e.g. 2010). The current spreadsheet does not 
have the data to calculate the citing half-life. A method to calculate the median age of the papers 
published in a topic that were cited in the reference year:

	1.	Choose a reference year, y (for example, 2010).
	2.	For each topic, t
		a.	Find the list, Lg of all papers in that topic published in year y
		b.	Find the list, Rg of all papers which are referenced in the papers in Lg
		c.	Find the set Yg of the year of publication of each paper in Rg 
		d.	Find mg, the median of Yg
		e.	Citing half-life of topic, t is  (y – mg + 1)

Prospective citation half-life
Prospective half-life is defined at the article level rather than the journal level. To determine prospective 
citation half-life of a paper, we decide a period, p from the date of publication of the paper (for example, 
20 year period). Then measure the total number of citations it received in that period. Half-life is determined 
as the period q (<p) in which half the citations were received. We have the data to measure this, if we use 
topics instead of articles. However, I think, replacing articles with topics has a problem that an 
article (or even a fixed set of articles) is static; once they are published they don’t change. On the other hand, 
a topic is dynamic with new articles continuously being added to the topic. While we may be able to provide 
this as one of the measures, and its measurement shows the combined result of citation 
longevity (which it is supposed to measure) and dynamism in the topic, I think, if we can, we should also 
include at least one of the other two (cited and citing half-life) measures.
 */
public class DBLPTopicQuery2 extends DBLPQuery {
	class TopicHalfLife implements Comparable {
		String topicId;
		int citedHalfLife;
		int citingHalfLife;
		int prospectiveHalfLife;
		TopicHalfLife(String t, int ctd, int ctng, int pros) {
			topicId = t;
			citedHalfLife = ctd;
			citingHalfLife = ctng;
			prospectiveHalfLife = pros;
		}
		@Override
		public int compareTo(Object arg0) {
			TopicHalfLife pd = (TopicHalfLife) arg0;
			return topicId.compareToIgnoreCase(pd.topicId);
		}
	}
	private String getSQL(String domainName, String shortname, int end) {
		// If I put distinct c.id - the result is different for a few topics - such as SE-74
		String papersRecvdCitation = "(SELECT c.id as id, p.year as year FROM dblp_citation_" + shortname +
		" c, dblp_pub_"+shortname+ " as p WHERE c.citation_year = " + end +
		" AND p.id = c.id )" ;
		String topicRecvdCitation = "SELECT td.tid as tid, pc.year as year FROM dblp_doctopic_" + shortname + 
		" AS td, " + papersRecvdCitation + " AS pc where td.id = pc.id ORDER BY td.tid, pc.year";
		return topicRecvdCitation;
	}
	/**
	 * Assumed that the list is already sorted
	 * @param list
	 * @return
	 */
	private int getMedian(ArrayList<Integer> list) {
		int len = list.size();
		int median= 0;
		if ((len%2)==0) {
			median = len/2;
		} else
			median = (len+1)/2;
		return list.get(median-1);
	}
	@Override
	public void processQuery(Connection conn, CsvWriter outfile)
	throws SQLException, IOException {
		String sql= getSQL(currResearchDomainDB, DBLPVenue.getResearchDomainShortName(currResearchDomainId), latestY);
		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		rs = st.executeQuery(sql);
		String topicid="";
		ArrayList<Integer> years =new ArrayList<Integer>();
		ArrayList<TopicHalfLife> topicDistribution= new ArrayList<TopicHalfLife>();
		while (rs.next()) {
			if (!rs.getString(1).equals(topicid) && !topicid.equals("")) {
				int medianYr = getMedian(years);
				int topicHalfLife = (latestY - medianYr)+1;
				topicDistribution.add(new TopicHalfLife(topicid, topicHalfLife, 0,0));
				years=null;
				years=new ArrayList<Integer>();
			}
			topicid= rs.getString(1);
			years.add(rs.getInt(2));
		}
		// The last one
		topicDistribution.add(new TopicHalfLife(topicid, 
				(latestY - getMedian(years) +1), 0,0));
		rs.close();
		st.close();
		outfile.write("TopicId"); 
		outfile.write("CitedHalfLife"); 
		outfile.write("CitingHalfLife"); 
		outfile.write("ProspectiveHalfLife"); 
		outfile.endRecord();
		for (TopicHalfLife t : topicDistribution) {
			outfile.write(t.topicId); 
			outfile.write(String.valueOf(t.citedHalfLife)); 
			outfile.write(String.valueOf(t.citingHalfLife)); 
			outfile.write(String.valueOf(t.prospectiveHalfLife)); 
			outfile.endRecord();
		}
	}
}
