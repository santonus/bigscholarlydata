package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.dblp.dbextractor.mining.DBLPTopicQuery1.PaperDistribution;

import com.csvreader.CsvWriter;

// This query was written per Dr. Sajeev's request. I couldn't run this in the command line as I couldn't generate a CSV file from command line.

public class DBLPRQ5Query extends DBLPQuery {

	@Override
	public void processQuery(Connection conn, CsvWriter outfile)
			throws SQLException, IOException {
		String sql= "SELECT pap.id as id, pap.title as title, pap.year as year, pap.citationcount as citationcnt, pap1.authcount as authcnt FROM " +
						" (SELECT p.id as id, p.title, p.year, count(*) as citationCount FROM mas_se_pub_citations c, " + this.currResearchDomainDB +
						" p WHERE p.id = c.pub_id " +
						" AND c.citation_title_year != '' AND p.year >= " + this.earliestY + 
						" AND p.year <= " + this.latestY +
						" GROUP BY p.id) as pap, " +
						" (SELECT p.id as id, count(a.author) as authcount from dblp_author_ref_new a, " + this.currResearchDomainDB +
						" p WHERE p.id = a.id AND p.year >= " + this.earliestY + 
						" AND p.year <= " + this.latestY +
						" GROUP BY p.id) as pap1 " +
						" WHERE pap.id = pap1.id";
		outfile.write("id");outfile.write("title");outfile.write("year");outfile.write("citationcnt");outfile.write("authcnt");
		outfile.endRecord();
		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		rs = st.executeQuery(sql);
		while (rs.next()) {
			outfile.write(String.valueOf(rs.getInt(1))); 
			outfile.write(rs.getString(2));
			outfile.write(String.valueOf(rs.getInt(3)));
			outfile.write(String.valueOf(rs.getInt(4)));
			outfile.write(String.valueOf(rs.getInt(5)));
			outfile.endRecord();
		}
		rs.close();
		st.close();

	}

}
