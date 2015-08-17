package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.dblp.dbextractor.DBLPVenue;

import com.csvreader.CsvWriter;

public class DBLPTopicAuthCollabQuery extends DBLPQuery {
	private boolean _topicBasedCollab=false;
	final public void setTopicBasedCollaboration() {
		_topicBasedCollab=true;
	}
	final public void setKWBasedCollaboration() {
		_topicBasedCollab=false;
	}

	@Override
	public void processQuery(Connection conn, CsvWriter outfile)
			throws SQLException, IOException {
		String sql = (_topicBasedCollab==false)?prepareForKWBasedQuery(outfile):prepareForTopicBasedQuery(outfile);
		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		rs = st.executeQuery(sql);
		while (rs.next()) {
			outfile.write(rs.getString(1));
			outfile.write(String.valueOf(rs.getInt(2)));
			outfile.write(String.valueOf(rs.getInt(3)));
			outfile.endRecord();
		}
		rs.close();
		st.close();
	}
	/**
	 * @param outfile
	 * @return
	 * @throws IOException
	 */
	private String prepareForKWBasedQuery(CsvWriter outfile) throws IOException {
		String shortname = DBLPVenue.getResearchDomainShortName(currResearchDomainId);
		String collbTableName = "dblp_authcollabkw_" +shortname; 
		String relTableName = "dblp_authkw_" +shortname;
		outfile.write("keyword");
		outfile.write("actualcollab");
		outfile.write("possiblecollab");
		outfile.endRecord();

		String view1 = "SELECT keyword AS keyword, count(*) as kwcollab FROM " + collbTableName + " GROUP BY keyword"; 
		String view2 = "SELECT keyword AS keyword, count(distinct author) as authcnt FROM " + relTableName + " GROUP BY keyword"; 

		String sql = "SELECT t1.keyword keyword, kwcollab, authcnt*(authcnt-1)/2 as possiblecollab FROM (" + view1 + ") AS t1, ("+
		view2+") AS t2 WHERE t1.keyword = t2.keyword ORDER BY possiblecollab";
		return sql;
	}
	/**
	 * @param outfile
	 * @return
	 * @throws IOException
	 */
	private String prepareForTopicBasedQuery(CsvWriter outfile)
			throws IOException {
		String shortname = DBLPVenue.getResearchDomainShortName(currResearchDomainId);
		String collbTableName = "dblp_authcollabkw_" +shortname; 
		String relTableName = "dblp_authkw_" +shortname;
		String topicTableName= "dblp_topic_" +shortname;
		outfile.write("topic");
		outfile.write("actualcollab");
		outfile.write("possiblecollab");
		outfile.endRecord();

		String view1 = "SELECT tid AS tid, count(*) as tcollab FROM " + collbTableName + " as kwcoll, " + topicTableName + " AS t " + 
		"WHERE t.keyword = kwcoll.keyword GROUP BY t.tid"; 
		String view2 = "SELECT tid AS tid, count(distinct author) as authcnt FROM " + relTableName + " AS kwrel, " + topicTableName +  " AS t " +
				"WHERE t.keyword = kwrel.keyword GROUP BY t.tid"; 

		String sql = "SELECT t1.tid topic, tcollab, authcnt*(authcnt-1)/2 as possiblecollab FROM (" + view1 + ") AS t1, ("+
		view2+") AS t2 WHERE t1.tid = t2.tid ORDER BY possiblecollab";
		return sql;
	}


}
