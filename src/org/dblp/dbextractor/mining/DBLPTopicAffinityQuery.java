package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;

import org.dblp.dbextractor.DBLPVenue;

import com.csvreader.CsvWriter;

public class DBLPTopicAffinityQuery extends DBLPQuery {

	@Override
	public void processQuery(Connection conn, CsvWriter outfile)
			throws SQLException, IOException {
		String topicListSql= "SELECT DISTINCT tid from dblp_topic_" + 
		DBLPVenue.getResearchDomainShortName(currResearchDomainId);
		Hashtable<String,Integer> topicNdx= new Hashtable<String,Integer>();
		String sql = DBLPSQLs.getAuthorTopicAffinityDistribution(currResearchDomainDB, 
				DBLPVenue.getResearchDomainShortName(currResearchDomainId), timeSlotBegin, timeSlotLast);
		outfile.write("author");

		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		rs = st.executeQuery(topicListSql);
		int i=0;
		while (rs.next()) {
			topicNdx.put(rs.getString(1), i);
			outfile.write(rs.getString(1));
			i++;
		}
		outfile.endRecord();
		rs.close();
		st.close();
		st= conn.createStatement();
		rs = st.executeQuery(sql);	
		int size= topicNdx.size();
		double [] affinityscore= new double[size];
		String currentAuthor="";
		// Assumed that the authors are sorted, implying that topic affinity for a given author
		// comes one after another.
		double affinityTotal=0.0;
		while (rs.next()) {
			String nextAuthor= rs.getString(1).trim();
			if (currentAuthor.equalsIgnoreCase(nextAuthor)==false) {
				writeAffinityForAuthor(outfile, affinityscore, currentAuthor, affinityTotal);
				for (i=0;i < affinityscore.length;i++)
					affinityscore[i]=0.0;
				currentAuthor= nextAuthor;
				affinityTotal=0.0;
			}
			int Ndx= topicNdx.get(rs.getString(2));
			affinityscore[Ndx]= rs.getDouble(3);
			affinityTotal += affinityscore[Ndx];
		}
		rs.close();
		st.close();
		writeAffinityForAuthor(outfile, affinityscore, currentAuthor,affinityTotal);
	}

	private void writeAffinityForAuthor(CsvWriter outfile,
			double[] affinityscore, String currentAuthor, double affinityTotal) throws IOException {
		int i;
		if (currentAuthor != "") {
			outfile.write(currentAuthor);
			for (i=0;i < affinityscore.length;i++)
				outfile.write(String.valueOf(affinityscore[i]/affinityTotal));
			outfile.endRecord();
		}
	}

}
