package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.TreeSet;

import org.dblp.dbextractor.DBLPVenue;

import com.csvreader.CsvWriter;
/*
 * In each time slice, how topics evolved, i.e. what is the freq. distribution of 
 * papers over topics in each time slice? 
 * Here for each time slice, we generate a freq. distribution of topics.
 */
public class DBLPTopicQuery1 extends DBLPQuery {
	public static final int BYPAPERCOUNT=0;
	public static final int BYCITATIONCOUNTHINDSIGHT=1;
	public static final int BYPAPERCOUNTBYYEAR=2;
	class PaperDistribution implements Comparable {
		String topicId;
		String topkws;
		int[] freq;
		PaperDistribution(String t, String kw, int n) {
			topicId = t;
			topkws = kw;
			freq = new int[n];
		}
		@Override
		public int compareTo(Object arg0) {
			PaperDistribution pd = (PaperDistribution) arg0;
			return topicId.compareToIgnoreCase(pd.topicId);
		}
	}
	private int _byCitation=0;
	public final void setTopicDistributionByPaperCount() {
		_byCitation=BYPAPERCOUNT;
	}
	public final void setTopicDistributionByCitationHindsight() {
		_byCitation=BYCITATIONCOUNTHINDSIGHT;
	}
	public final void setTopicDistributionByCitationInAYear() {
		_byCitation=BYPAPERCOUNTBYYEAR;
	}

	@Override
	public void processQuery(Connection conn, CsvWriter outfile)
	throws SQLException, IOException {
		outfile.write("topicid");
		outfile.write("top kws");
		int numsteps=0;
		for (int curryr = earliestY; curryr <= latestY; curryr+= timestep) {
			int endyr= (curryr + timestep-1)>latestY?latestY:(curryr+timestep-1);
			String yrRange = curryr + "-" + endyr;
			outfile.write(yrRange);
			numsteps++;
		}
		outfile.endRecord();
		Hashtable<String,PaperDistribution> topicDistribution= new Hashtable<String,PaperDistribution>();
		int stepcount=0;
		for (int curryr = earliestY; curryr <= latestY; curryr+= timestep) {
			int endyr= (curryr + timestep-1)>latestY?latestY:(curryr+timestep-1);
			String sql= null;
			switch(_byCitation) {
			case BYPAPERCOUNT:
				sql=DBLPSQLs.getTopicDistributionByPaperCount(currResearchDomainDB, 
						DBLPVenue.getResearchDomainShortName(currResearchDomainId), curryr, endyr);
				break;
			case BYCITATIONCOUNTHINDSIGHT:
				sql=DBLPSQLs.getTopicDistributionByCitation(currResearchDomainDB, 
						DBLPVenue.getResearchDomainShortName(currResearchDomainId), curryr, endyr);
				break;
			case BYPAPERCOUNTBYYEAR:
				sql=DBLPSQLs.getTopicDistributionByCitationVariation(currResearchDomainDB, DBLPVenue.getResearchDomainShortName(currResearchDomainId), 
						curryr, endyr);
				break;
			default: return;
			}
			Statement st;
			ResultSet rs;
			st= conn.createStatement();
			rs = st.executeQuery(sql);
			String topicid="";
			StringBuffer topkws=new StringBuffer();
			int papercount=0;
			while (rs.next()) {
				if (!rs.getString(1).equals(topicid) && !topicid.equals("")) {
					PaperDistribution paperDist= topicDistribution.get(topicid);
					topkws.trimToSize();
					if (paperDist==null ) {
						paperDist= new PaperDistribution(topicid, topkws.toString(), numsteps);
						topicDistribution.put(topicid, paperDist);
					}
					paperDist.freq[stepcount]= papercount;
					topkws=null;
					topkws=new StringBuffer();
				}
				topicid= rs.getString(1);
				topkws.append(rs.getString(2) + " ");
				papercount=rs.getInt(3);
			}
			rs.close();
			st.close();
			// Last Topic
			PaperDistribution paperDist= topicDistribution.get(topicid);
			topkws.trimToSize();
			if (paperDist==null ) {
				paperDist= new PaperDistribution(topicid, topkws.toString(), numsteps);
				topicDistribution.put(topicid, paperDist);
			}
			paperDist.freq[stepcount]= papercount;
			topkws=null;
			stepcount++;
		}
		TreeSet<PaperDistribution> sortedSet = new TreeSet<PaperDistribution>(topicDistribution.values());
		for (PaperDistribution pd : sortedSet) {
			outfile.write(pd.topicId); 
			outfile.write(pd.topkws); 
			for (stepcount=0; stepcount < numsteps; stepcount++)
				outfile.write(String.valueOf(pd.freq[stepcount]));
			outfile.endRecord();
		}
	}
}
