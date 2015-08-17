package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.csvreader.CsvWriter;

public class DBLPQuery0 extends DBLPQuery {

	@Override
	public void processQuery(Connection conn, CsvWriter outfile)
			throws SQLException, IOException {
		String sql = DBLPSQLs.getSummary(currResearchDomainDB);
		int[] res = getTotalPapersAuthors(conn); 
		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		rs = st.executeQuery(sql);
		int totcount=0;
		outfile.write("Total Papers"); outfile.write("Total Authors"); 
		outfile.endRecord();
		outfile.write(Integer.toString(res[0])); outfile.write(Integer.toString(res[1]));
		outfile.endRecord();
		outfile.write("Venue"); outfile.write("VenueId"); 
		outfile.write("NumPapers"); outfile.write("%NumPapers"); outfile.write("NumAuth"); outfile.write("%NumAuths");
		outfile.write("Inception");
		outfile.endRecord();
		while (rs.next()) {
			outfile.write(rs.getString(1)); outfile.write(rs.getString(2)); 
			outfile.write(Integer.toString(rs.getInt(3))); outfile.write(Double.toString(((double)rs.getInt(3))*100.0/(double)res[0]));
			outfile.write(Integer.toString(rs.getInt(4))); outfile.write(Double.toString(((double)rs.getInt(4))*100.0/(double)res[1]));
			outfile.endRecord();
		}
		rs.close(); st.close();
	}
	private int[] getTotalPapersAuthors(Connection conn) throws SQLException {
		String totPaper = "SELECT COUNT(*) FROM " + currResearchDomainDB;
		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		rs = st.executeQuery(totPaper);
		int[] res= new int[2];
		rs.next();
		res[0]= rs.getInt(1);
		rs.close(); st.close();
		String totAuth = "SELECT COUNT(DISTINCT author) FROM " + currResearchDomainDB + " p, dblp_author_ref_new a " +
		"WHERE p.id=a.id";
		st= conn.createStatement();
		rs = st.executeQuery(totAuth);
		rs.next();
		res[1]= rs.getInt(1);
		return res;
	}
}
