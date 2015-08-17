package org.dblp.dbextractor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.dblp.dbextractor.mining.DBLPSQLs;
import org.dblp.model.BibCluster;
import org.dblp.model.BibEntry;
import org.dblp.model.ClusterMatch;

public class DataManager {
	private Connection _conn;
	private String _appBase;
	HashSet<BibEntry> _bibEntries= null;
	public final String getAppBase() {
		return _appBase;
	}
	public final void setAppBase(String appBase) {
		_appBase = appBase;
	}
	public final Connection getConnection() {
		return _conn;
	}
	public void connect(String url) {
		try {
			Class.forName(DBConstants.DB_CLS_DERBY);
			String dbstring = "jdbc:derby:" + url; 
			_conn = DriverManager.getConnection(dbstring);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public void connectmySql(String dbName) {
		try {
			Class.forName(DBConstants.DB_CLS_MYSQL);
			String dbstring = "jdbc:mysql://localhost/" + dbName + "?user=root&password="; 
			_conn = DriverManager.getConnection(dbstring);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	public void closeConnection() {
		if (_conn != null) {
			try {
				_conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public void setupMysqlDB(int id) {
/*			StringBuffer venues= new StringBuffer("source_id like '" + 
					DBLPVenue.SEVENUE[0] +"' ");
			for (int i=1; i < DBLPVenue.SEVENUE.length;i++) {
				venues.append("OR source_id like '" + DBLPVenue.SEVENUE[i] +"'" ); 
			}
			for (int i=1; i < DBLPVenue.SEVENUEEXTRA.length;i++) {
				venues.append("OR source like '" + DBLPVenue.SEVENUEEXTRA[i] +"'" ); 
			}

			String sql= "select * from dblp_pub_new where " + venues.toString(); */
			String crviewsql = DBLPVenue.getVenueIDAsSQL(id);
				//"create or replace view dblp_pub_se as " + sql;
			try {
				Statement st= _conn.createStatement();
				st.executeUpdate(crviewsql);
				st.close();
				prepareForCrawledDataImport(id);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	private void prepareForCrawledDataImport(int id) throws SQLException {
		String shortname = DBLPVenue.getResearchDomainShortName(id);
		String crAbstrTable = DBLPSQLs.getAbstractTableCreationSQL(shortname);
		String crCiteRawTable = DBLPSQLs.getRawCitationRefTableCreationSQL(shortname);	
		String crCiteTable = DBLPSQLs.getRawCitationTableCreationSQL(shortname);

		Statement st=_conn.createStatement();
		st.executeUpdate(crAbstrTable);
		st.close();
		st=_conn.createStatement();
		st.executeUpdate(crCiteRawTable);
		st.close();

		st=_conn.createStatement();
		st.executeUpdate(crCiteTable);
		st.close();
	}

	public int[] getMinMaxYearForResearchDomain(int id) throws SQLException {
		return getMinMaxYear(DBLPVenue.getResearchDomainViewName(id));
	}
	
	private int[] getMinMaxYear(String pubTable) throws SQLException {
		String sql = " select min(year), max(year) from " + pubTable + " where year > 0";
		Statement st= _conn.createStatement();
		ResultSet rs = st.executeQuery(sql);
		rs.next();
		int minmax[] = new int[2];
		minmax[0]= rs.getInt(1); // Starting year
		minmax[1]= rs.getInt(2); // latest year
		return minmax;
	}
	public void insertClusters(HashMap<String, BibCluster> clusterDetails) {
		String insertSql = "INSERT INTO CLUSTER (CLUSTERID, CLUSTERNAME, TOPWORDS) VALUES (?,?, ?)";
		PreparedStatement st;
		ResultSet rs;
		int count=0;
		try {
			st = _conn.prepareStatement(insertSql);
			int clusterId=0;
			for (String clusterName : clusterDetails.keySet()) {
				BibCluster aCluster = clusterDetails.get(clusterName);
				HashSet<String> topwords = aCluster.getTopWords();
				String topWordStr = topwords.toString();
				clusterId++;
				st.setInt(1, aCluster.getClusterId());
				st.setString(2, aCluster.getClusterName());
				st.setString(3, topWordStr);
				st.addBatch();
			}
			st.executeBatch();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	// DONT EXECUTE THIS. Table name mismatch is there.
	public void insertClusterMatch(HashSet<ClusterMatch> matches) {
		String insertSql = "INSERT INTO CLUSTER_ENTRY_REL" +
				" (CLUSTERID, ENTRYID, SIMILARITY) VALUES (?,?, ?)";
		PreparedStatement st;
		ResultSet rs;
		int count=0;
		try {
			st = _conn.prepareStatement(insertSql);
			int clusterId=0;
			for (ClusterMatch aMatch : matches) {
				st.setInt(1, aMatch.getCluster().getClusterId());
				st.setInt(2, Integer.valueOf(aMatch.getBib().getEntryId()));
				st.setDouble(3, aMatch.getSimilarity());
				st.addBatch();
			}
			st.executeBatch();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Set<BibEntry> getEntrySet() {
		return _bibEntries;
	}

}
