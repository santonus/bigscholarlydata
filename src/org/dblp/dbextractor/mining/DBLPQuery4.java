package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.csvreader.CsvWriter;

public class DBLPQuery4 extends DBLPQuery {

	@Override
	public void processQuery(Connection conn, CsvWriter outfile)
			throws SQLException, IOException {
		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		outfile.write("author"); 
		outfile.write("totalPaper"); 
		outfile.write("firstYear"); 
		outfile.write("lastYear"); 
		outfile.write("VenueCnt"); 
		
		outfile.write("CoauthPapCnt"); 
		outfile.write("distinctCoauthCnt"); 
		outfile.endRecord();
		
		String table1 = "( " + DBLPSQLs.getAuthorDetails(this.currResearchDomainDB, earliestY, latestY) + ") as t1";
		String table2 = "( " + DBLPSQLs.getCoAuthoredPaperCount(this.currResearchDomainDB,earliestY, latestY) + ") as t2";
		String table3 = "( " + DBLPSQLs.getDistinctAuthCount(this.currResearchDomainDB, earliestY, latestY) + ") as t3";
		
	/*	String q4Sql = "SELECT t1.author, t1.pcount, t1.miny, t1.maxy, t1.venuecnt, t2.coauthpcnt, t3.distauthcnt " +
		"FROM " + table1 + ", " + table2 + ", " + table3 + 
		" WHERE t1.author= t2.author AND t1.author = t3.author";
*/		
		String q4Sql = "SELECT t1.author, t1.pcount, t1.miny, t1.maxy, t1.venuecnt, IFNULL(t2.coauthpcnt,0), t3.distauthcnt " +
		"FROM " + table1 + " LEFT JOIN " + table2 + " ON t1.author= t2.author, " + table3 + 
		" WHERE t1.author= t3.author";
		rs = st.executeQuery(q4Sql);
		int total=0;
		while (rs.next()) {
			outfile.write(rs.getString(1));
			outfile.write(Integer.toString(rs.getInt(2)));
			outfile.write(Integer.toString(rs.getInt(3)));
			outfile.write(Integer.toString(rs.getInt(4)));
			outfile.write(Integer.toString(rs.getInt(5)));
			outfile.write(Integer.toString(rs.getInt(6)));
			outfile.write(Integer.toString(rs.getInt(7)));
			outfile.endRecord();
			total++;
		}
		rs.close(); st.close();
		System.out.println("Total records=" + total);
	}

}
