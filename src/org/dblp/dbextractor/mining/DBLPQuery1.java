package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.csvreader.CsvWriter;

/*
 * In each time slice, how many authors published 1 paper, how many 2
	papers, how many 3 or more papers?
 */
public class DBLPQuery1 extends DBLPQuery{
	private int cutoff; // e.g. 3 publications or more
	private int[] paperAuthCnt= null;
	public DBLPQuery1(int thr, int step) {
		cutoff= thr;
		setTimeStep(step);
	}
	public int getCutoff() {
		return cutoff;
	}
	
	public int[] executeQuery(Connection conn, int startYear, int endYear) throws SQLException {
		String sql = DBLPSQLs.getAuthPubCountInTimeSlice(currResearchDomainDB, startYear, endYear);
		paperAuthCnt = new int[cutoff];
		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		rs = st.executeQuery(sql);
		while (rs.next()) {
			int papcount = rs.getInt(1);
			int authcount = rs.getInt(2);
			if (papcount < cutoff)
				paperAuthCnt[papcount-1] = authcount; // Since array index starts from 0
			else
				paperAuthCnt[cutoff-1] += authcount;
		}
		rs.close(); st.close();
		return paperAuthCnt;
	}
	
	public void processQuery(Connection conn, CsvWriter outfile) throws SQLException, IOException{
		outfile.write("Range"); 
		for (int i=1; i < cutoff; i++)
			outfile.write("TotAuth with PCount=" + i);
		outfile.write("TotAuth with PCount-" + cutoff + "+");
		outfile.endRecord();
		for (int curryr = earliestY; curryr <= latestY; curryr+=timestep) {
			int endyr= (curryr + timestep-1)>latestY?latestY:(curryr+timestep-1);
			int[] res = executeQuery(conn, curryr, endyr);
			String yrRange = curryr + "-" + endyr;
			outfile.write(yrRange);
			for (int i=0; i < cutoff; i++)
				outfile.write(Integer.toString(res[i]));
			outfile.endRecord();
		}
	}
}
