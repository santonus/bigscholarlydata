package org.dblp.dbextractor;



import java.sql.*;
import java.util.LinkedList;
import java.util.List;

import org.dblp.crawler.model.PaperDetails;
import org.dblp.crawler.model.PublishDetails;

public class PaperExtraInfoDBHandler {
	private Connection _conn = null;
	
	public PaperExtraInfoDBHandler(Connection c) {
		setConnection(c);
	}
	public final void setConnection(Connection c) {
		_conn = c;
	}

/*	public static void main(String[] args) {
		getConnection();

		//getPublishDetails_doi_acm_org();
		
		

		// savePaperDetails( new PaperDetails(1, "www.test.com", 1, "test abstract") );

		closeConnection();
	} */

/*	public static void getConnection() {
		try {
			String url = "jdbc:mysql://localhost/" + DBConstants.DB_NAME;
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(url, DBConstants.DB_USERNAME, DBConstants.DB_PWD);
			System.out.println("Database connection established");
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
*/	
	public List<PublishDetails> getPublishDetails_all(int id) {
		String query = "SELECT id, doi, ee FROM " + 
		DBLPVenue.getResearchDomainViewName(id) + " p order by p.ee";
		return getPublishDetails(query);
	}

	public List<PublishDetails> getPublishDetails(String query) {
		List<PublishDetails> publishDetails = new LinkedList<PublishDetails>();

		int idVal = 0;
		String doiVal = null;
		String ee = null;
		int count = 0;

		try {
			Statement s = _conn.createStatement();
			s.executeQuery(query);
			ResultSet rs = s.getResultSet();

			while (rs.next()) {
				idVal = rs.getInt("id");
				doiVal = rs.getString("doi");
				ee = rs.getString("ee");
				// System.out.println(idVal + " " + doiVal);

				publishDetails.add(new PublishDetails(idVal, doiVal, ee));

				++count;
			}

			rs.close();
			s.close();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		System.out.println(count + " rows were retrieved");

		return publishDetails;
	}

	public boolean doesExistIn_dblp_pprdet(int refId) {
		boolean exists = false;

		String query = "SELECT * FROM dblp_pprdet d where d.refId = " + refId;

		int count = 0;

		try {
			Statement s = _conn.createStatement();
			s.executeQuery(query);
			ResultSet rs = s.getResultSet();

			while (rs.next()) {
				++count;
			}

			rs.close();
			s.close();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}

		if (count > 0)
			exists = true;
		else
			exists = false;

		return exists;
	}

	public List<PaperDetails> getPaperDetailsToBeDone() {
		String query = "select * from dblp_pprdet where citationCount is null order by generatedURL";
		
		List<PaperDetails> paperDetails = new LinkedList<PaperDetails>();
		
		int refId = 0;
		String generatedURL = null;
		int count = 0;
		
		try {
			Statement s = _conn.createStatement();
			s.executeQuery(query);
			ResultSet rs = s.getResultSet();

			while (rs.next()) {
				refId = rs.getInt("refId");
				generatedURL = rs.getString("generatedURL");
				
				// System.out.println(refId + " " + generatedURL);

				paperDetails.add(new PaperDetails(refId, generatedURL, -1, null));

				++count;
			}

			rs.close();
			s.close();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		System.out.println(count + " rows were retrieved");

		return paperDetails;
	}
	
	public void savePaperDetailsAll(PaperDetails paperDetail) {
		String query = "insert into dblp_pprdet(refId, generatedURL, citationCount, paperAbstract) values(" + paperDetail.getRefId() + ", '" + paperDetail.getGeneratedURL()
				+ "', " + paperDetail.getCitationCount() + ", '" + paperDetail.getSafePaperAbstract() + "');";

		// System.out.println(query);

		savePaperDetails(query);
	}
	
	public void savePaperDetailsURL(PaperDetails paperDetail) {
		String query = "insert into dblp_pprdet(refId, generatedURL) values(" + paperDetail.getRefId() + ", '" + paperDetail.getGeneratedURL() + "');";
		
		savePaperDetails(query);
	}
	
	public void savePaperDetailsCitationAbstract(PaperDetails paperDetail) {
		String query = "update dblp_pprdet set citationCount = " + paperDetail.getCitationCount() + ", paperAbstract = '" + paperDetail.getSafePaperAbstract() + "' where refId = " + paperDetail.getRefId();
		
		//System.out.println(query);
		
		savePaperDetails(query);
	}

	public void savePaperDetails(String query) {
		try {
			Statement s = _conn.createStatement();

			s.executeUpdate(query);

			s.close();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void closeConnection() {
		if (_conn != null) {
			try {
				_conn.close();
				System.out.println("Database connection terminated");
			}
			catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}
}
