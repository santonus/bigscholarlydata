package org.dblp.dbextractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class DBLPSanityTest {

	/*
	 * dblp_pub_new
	+----------------+-----------------+------+-----+---------+----------------+
	| Field          | Type            | Null | Key | Default | Extra          |
	+----------------+-----------------+------+-----+---------+----------------+
	| id             | int(8)          | NO   | PRI | NULL    | auto_increment |
	| dblp_key       | varchar(150)    | NO   | MUL |         |                |
	| title          | longtext        | NO   | MUL | NULL    |                |
	| source         | varchar(150)    | YES  | MUL | NULL    |                |
	| source_id      | varchar(50)     | YES  | MUL | NULL    |                |
	| series         | varchar(100)    | YES  |     | NULL    |                |
	| year           | int(4) unsigned | NO   |     | 0       |                |
	| type           | varchar(20)     | NO   | MUL |         |                |
	| volume         | varchar(50)     | YES  |     | NULL    |                |
	| number         | varchar(20)     | YES  |     | NULL    |                |
	| month          | varchar(30)     | YES  |     | NULL    |                |
	| pages          | varchar(100)    | YES  |     | NULL    |                |
	| ee             | varchar(200)    | YES  |     | NULL    |                |
	| ee_PDF         | varchar(200)    | YES  |     | NULL    |                |
	| url            | varchar(150)    | YES  |     | NULL    |                |
	| publisher      | varchar(250)    | YES  |     | NULL    |                |
	| isbn           | varchar(25)     | YES  |     | NULL    |                |
	| crossref       | varchar(50)     | YES  | MUL | NULL    |                |
	| titleSignature | varchar(255)    | YES  | MUL | NULL    |                |
	| doi            | varchar(255)    | YES  |     | NULL    |                |
	| mdate          | date            | NO   |     | NULL    |                |
	+----------------+-----------------+------+-----+---------+----------------+
	 ** dblp_author_ref_new
	+------------+-------------+------+-----+---------+-------+
	| Field      | Type        | Null | Key | Default | Extra |
	+------------+-------------+------+-----+---------+-------+
	| id         | int(8)      | NO   | PRI | NULL    |       |
	| author     | varchar(70) | NO   | PRI |         |       |
	| editor     | int(1)      | NO   |     | 0       |       |
	| author_num | int(3)      | NO   |     | NULL    |       |
	+------------+-------------+------+-----+---------+-------+

	 ** dblp_ref_new
	+--------+--------------+------+-----+---------+-------+
	| Field  | Type         | Null | Key | Default | Extra |
	+--------+--------------+------+-----+---------+-------+
	| id     | int(8)       | NO   | PRI | NULL    |       |
	| ref_id | varchar(150) | NO   | PRI |         |       |
	+--------+--------------+------+-----+---------+-------+

	 */
	public static void testDBLPDump(Connection conn) throws Exception{
		String sql = "SELECT count(*) from dblp_author_ref_new";
		String sql1 = "SELECT count(*) from dblp_ref_new";
		String sql2 = "SELECT count(*) from dblp_pub_new";

		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		rs = st.executeQuery(sql);
		rs.next();
		System.out.println("Number of authors=" + rs.getInt(1));
		rs.close(); st.close();

		st= conn.createStatement();
		rs = st.executeQuery(sql2);
		rs.next();
		System.out.println("Number of xref=" + rs.getInt(1));
		rs.close(); st.close();

		st= conn.createStatement();
		rs = st.executeQuery(sql2);
		rs.next();
		System.out.println("Number of publications=" + rs.getInt(1));
		System.out.println("------------------------------");
		rs.close(); st.close();
	}
	public static void testVenueDetails(Connection conn, int id) throws Exception{
		String viewName= DBLPVenue.getResearchDomainViewName(id);
//		String whereClause= DBLPVenue.getVenuesAsWhereClause(id).toString();
		String sql = "SELECT source_id, source, COUNT(*) FROM " + viewName + " GROUP BY source_id,source";

		Statement st;
		ResultSet rs;
		st= conn.createStatement();
		rs = st.executeQuery(sql);
		System.out.println("VenueId Venue              NumPapers");
		System.out.println("------------------------------");
		int count=0;
		while(rs.next()) {
			System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getInt(3));
			count += rs.getInt(3);
		}
		System.out.println("------------------------------\n Total=" + count);
		rs.close(); st.close();
	}

}
