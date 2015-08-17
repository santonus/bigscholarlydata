package org.dblp.dbextractor.mining;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

import org.dblp.util.RandomGenerator;

import com.csvreader.CsvWriter;
/*
 * currAuth	coAuth	1. papDiff	2. coAuthDiff 3. venueDiff	
 * 4. spanDiff	5. commonTitleStrings	6. pastColabCnt	7. colabInCurrTS
 */
public class DBLPQuery28 extends DBLPQuery {
/*
 * (non-Javadoc)
 * @see org.dblp.dbextractor.mining.DBLPQuery#processQuery(java.sql.Connection, com.csvreader.CsvWriter)
 * 1. Oldauthors = All old authors for the time-step. Store this in a tmp table
 * 2. OldOldPastLink = create a sql query to get this.
 * 3. OldOldNewLink = use the same sql with current time step to get this.
 * 4. OldauthorsDetails = use tmp table and the complex sql to get this.
 * 5. For an author oi= Oldauthors[i], i = 1..n
 * 5.1 for another old author oj= Oldauthors[j], j=i+1..n
 * 5.1.1. get o1 details from oldauthorsDetails. and get o2 details from oldauthorsDetails
 * 5.1.2 do subtraction on all field.
 * 5.1.3 if (o1, o2) exists in OldOldPastLink, put the number, else 0
 * 5.1.4 if (o1. o2) exists in OldOldNewLink, put true else false
 */
	class AuthorDetails {
		int totalPaper;
		int firstYear; 
		int lastYear; 
		int VenueCnt; 
		int CoauthPapCnt; 
		int distinctCoauthCnt; 
		int getSpan() {
			if (lastYear > 0 && firstYear > 0)
				return (lastYear-firstYear)+1;
			else
				return 0;
		}
	}
	
	private Hashtable<String,AuthorDetails> _authortable;
	private double _noCollabPercentage=1.0;
	private double _pd[]= {_noCollabPercentage+0,(1.0-_noCollabPercentage)};
	public void setNoCollabGenerationPercentage(double val) {
		_noCollabPercentage =val;
		_pd[0]= _noCollabPercentage; _pd[1]= (1.0 - _noCollabPercentage);
	}
	@Override
	public void processQuery(Connection conn, CsvWriter outfile)
			throws SQLException, IOException {
		Hashtable<String,AuthorDetails> authortable = getOldAuthorDetails(conn);
		System.out.println("Total Old Auths=" + authortable.size());
		String oldAuths[]= authortable.keySet().toArray(new String[0]);
		Hashtable<String, Hashtable<String, Integer>> oldOldPastRel= new Hashtable<String, Hashtable<String, Integer>>();
		System.out.println("Total Old-Old relationship before " + timeSlotBegin + " is:" + 
				getOldOldRel(conn, oldOldPastRel, true));
		Hashtable<String, Hashtable<String, Integer>> oldOldCurrRel= new Hashtable<String, Hashtable<String, Integer>>();
		System.out.println("Total Old-Old relationship during " + timeSlotBegin + " and " 
				+ timeSlotLast + " is:" + getOldOldRel(conn, oldOldCurrRel, false));
		// DO SANITY CHECK
		Set<String> keys= oldOldCurrRel.keySet();
		for (String key: keys) {
			Hashtable<String,Integer> subrels= oldOldCurrRel.get(key);
			Set<String> subkeys= subrels.keySet();
			if (authortable.get(key)==null)
				System.out.println("ERROR: Author " + key + "not found in author master");
			for (String subkey: subkeys) {
				if (authortable.get(subkey)==null)
					System.out.println("ERROR: Co-Author " + subkey + "not found in author master for author" + key);
			}
		}
		//printRelation(oldOldCurrRel);
		//END SANITY CHECK
		outfile.write("author"); 
		outfile.write("coauthor");
		outfile.write("papDiff"); 
		outfile.write("coAuthDiff"); 
		outfile.write("venueDiff"); 
		outfile.write("spanDiff"); 
		outfile.write("pastColabCnt"); 
		outfile.write("colabInCurrTS"); 
		outfile.endRecord();
		int total =0;
		System.out.println("Will generate " + _noCollabPercentage *100 + "% of all author-pairs who never collaborated earlier and in current timestep");
		RandomGenerator rand = new RandomGenerator(_pd);
		for (int i= 0; i < oldAuths.length; i++) {
			String author = oldAuths[i];
			if (author == null || authortable.get(author)==null) {
				System.out.println("ERROR: Author details NULL for <" +author + ">, at index" + i);
				continue;
			}
			for (int j= i+1; j < oldAuths.length; j++) {
				String coAuth = oldAuths[j];
				if (coAuth == null || authortable.get(coAuth)==null) {
					System.out.println("ERROR: Co-Author details NULL for <" +coAuth + ">, at index" + i);
					continue;
				}
				Hashtable<String, Integer> pastCollab = oldOldPastRel.get(author);
				Hashtable<String, Integer> currCollab = oldOldCurrRel.get(author);

				int pastCollabCount= 0;
				if (pastCollab != null && pastCollab.get(coAuth) != null)
					pastCollabCount= pastCollab.get(coAuth);
				int currCollabCount= 0;
				if (currCollab != null && currCollab.get(coAuth) != null)
					currCollabCount= currCollab.get(coAuth);
				if (pastCollabCount == 0 && currCollabCount==0) {
					if (rand.getNextElementNdx()==1) // Should we generate this record at all?
						continue; 
				}
				outfile.write(author); 
				outfile.write(coAuth);
				outfile.write(Integer.toString(Math.abs(authortable.get(author).totalPaper - 
						authortable.get(coAuth).totalPaper)));  
				outfile.write(Integer.toString(Math.abs(authortable.get(author).distinctCoauthCnt - 
						authortable.get(coAuth).distinctCoauthCnt)));  
				outfile.write(Integer.toString(Math.abs(authortable.get(author).VenueCnt - 
						authortable.get(coAuth).VenueCnt)));  
				outfile.write(Integer.toString(Math.abs(authortable.get(author).getSpan() - 
						authortable.get(coAuth).getSpan())));
				outfile.write(Integer.toString(pastCollabCount));  
				outfile.write(Integer.toString(currCollabCount));  
				outfile.endRecord();
				total++;
			}
		}
		cleanupForQ28(conn);
		System.out.println("Total records=" + total);
	}
	
	private Hashtable<String,AuthorDetails> getOldAuthorDetails(Connection conn) 
	throws SQLException, IOException {
		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		String oldauthcreateSql = DBLPSQLs.getOldAuthorsInTimeSlice(this.currResearchDomainDB, timeSlotBegin, timeSlotLast);
		st= conn.createStatement();
		st.executeUpdate(oldauthcreateSql);
		st.close();
		st = conn.createStatement();
		rs = st.executeQuery("select count(*) from tmpt");
		rs.next();
		int oldAuthCount= rs.getInt(1);
		rs.close(); 
		st.close();

		Hashtable<String,AuthorDetails> authDetails= new Hashtable<String,AuthorDetails>();
		String table1 = "( " + DBLPSQLs.getAuthorDetails(this.currResearchDomainDB, earliestY, timeSlotBegin-1) + ") as t1";
		String table2 = "( " + DBLPSQLs.getCoAuthoredPaperCount(this.currResearchDomainDB, earliestY, timeSlotBegin-1) + ") as t2";
		String table3 = "( " + DBLPSQLs.getDistinctAuthCount(this.currResearchDomainDB, earliestY, timeSlotBegin-1) + ") as t3";
		
		String sql = "SELECT t1.author, t1.pcount, t1.miny, t1.maxy, t1.venuecnt, IFNULL(t2.coauthpcnt,0), t3.distauthcnt " +
		"FROM " + table1 + " LEFT JOIN " + table2 + " ON t1.author= t2.author, " + table3 + ", tmpt " +
		" WHERE t1.author= t3.author AND t1.author = tmpt.ca";
		st= conn.createStatement();
		rs = st.executeQuery(sql);
		int oldAuthDtlCount=0;
		while (rs.next()) {
			String authName = rs.getString(1);
			if (authDetails.get(authName) != null) 
				throw new SQLException("Mistake, Same author <" + authName + "> appears more than 1. Check SQL=" + sql);
			AuthorDetails thisAuthDetails = new AuthorDetails();
			thisAuthDetails.totalPaper = rs.getInt(2);
			thisAuthDetails.firstYear = rs.getInt(3);
			thisAuthDetails.lastYear = rs.getInt(4);
			thisAuthDetails.VenueCnt= rs.getInt(5);
			thisAuthDetails.CoauthPapCnt = rs.getInt(6);
			thisAuthDetails.distinctCoauthCnt = rs.getInt(7);
			authDetails.put(authName, thisAuthDetails);
			oldAuthDtlCount++;
		}
		assert (oldAuthDtlCount==oldAuthCount);
		return authDetails;
	}
	private int getOldOldRel(Connection conn, 
			Hashtable<String, Hashtable<String, Integer>> ooRel, boolean past)
	throws SQLException, IOException {
//		Hashtable<String, Hashtable<String, Integer>> ooRel= new Hashtable<String, Hashtable<String, Integer>>();
		Statement st= conn.createStatement();
		String sql = null;
		if (past==true) 
			sql = DBLPSQLs.getOldOldCollaboration(this.currResearchDomainDB, earliestY, timeSlotBegin-1);
		else
			sql = DBLPSQLs.getOldOldCollaboration(this.currResearchDomainDB, timeSlotBegin, timeSlotLast);
		ResultSet rs = st.executeQuery(sql);
		int totrec=0;
		while (rs.next()) {
			String authName = rs.getString(1);
			String coauthName = rs.getString(2);
			int count = rs.getInt(3);
			Hashtable<String, Integer> coauthRel = ooRel.get(authName);
			if (coauthRel == null) {
				coauthRel = new Hashtable<String, Integer>();
				ooRel.put(authName, coauthRel);
			}
			if (coauthRel.get(coauthName) != null)
				throw new SQLException("Mistake, Same co-author <" + coauthName + "> appears more than 1. Check SQL=" + sql);
			coauthRel.put(coauthName, rs.getInt(3));
			totrec++;
		}
		rs.close(); st.close();
		return totrec;
	}
	private void cleanupForQ28(Connection conn) throws SQLException {
		Statement st = conn.createStatement();
		st.executeUpdate("drop table tmpt");
	}
	private void printRelation(Hashtable<String, Hashtable<String, Integer>> relation) {
		Set<String> keys= relation.keySet();
		for (String key: keys) {
			Hashtable<String,Integer> subrels= relation.get(key);
			Set<String> subkeys= subrels.keySet();
			for (String subkey: subkeys)
				System.out.println(key + ", " + subkey + ", " + subrels.get(subkey));
		}
	}
}
