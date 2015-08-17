package org.dblp.dbextractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dblp.dbextractor.mining.DBLPSQLs;
import org.dblp.model.BibEntry;
import org.dblp.topicmining.TopicConstants;

public class DataExporterImporter {
	public final int[] exportPaperAbstracts(Connection conn, int domainId, int minY, int maxY, 
			boolean rawData, String dirPath) {
		String nullAbstract1= "An abstract is not available.";
		String nullAbstract2= "Abstract Not Found";
		String sql = DBLPVenue.getResearchDomainAbstractSQL(domainId, minY, maxY);
//		sql= "SELECT p.id, p.title, abs.paperAbstract FROM " + DBLPVenue.getResearchDomainViewName(domainId) + 
//		" AS p, dblp_pprdet AS abs WHERE " +
//		"p.id = abs.refId AND p.year >= " + minY + " and p.year <= " + maxY ;
		Statement st;
		ResultSet rs;
		int count=0;
		int nonNullAbs=0;
		try {
			st = conn.createStatement();
			rs = st.executeQuery(sql);
			File file = new File(dirPath);
			file.mkdirs();
			boolean nullAbs=false;
			while (rs.next()) {
				String filename= dirPath + File.separator + String.valueOf(rs.getInt(1)) + ".txt";
				String text = null;
				String title= rs.getString(2).trim();
				String abs= rs.getString(3) != null?rs.getString(3).trim():null;
				if (abs == null || abs.equalsIgnoreCase(nullAbstract1) || abs.equalsIgnoreCase(nullAbstract2)) {
					nullAbs= true;
					nonNullAbs++;
				}
				try {
				if (rawData == true) {
					text = (nullAbs==false)?(title + "\n" + abs):title ;
				} else { // Do stemming and stopword removal
					BibEntry anEntry= new BibEntry();
					anEntry.setEntryId(String.valueOf(rs.getInt(1)));
					anEntry.setTitle(title);
					if (nullAbs==false)
						anEntry.setAbstract(abs);
					text = (nullAbs==false)?(anEntry.getTitleAsBOW() + "\n " + anEntry.getAbstractBOW())
							:(anEntry.getTitleAsBOW()); 
					anEntry = null; // Not very memory efficient, but okay for now.
				}
				writeTextIntoFile(filename, text);
				count++;
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int[] res= {count, nonNullAbs};
		return res;
	}
	public final int importTopics(Connection conn, int domainId, int minY, int maxY, String docTopicFile, String topicKWFile) {
		int count=0;
		
		String shortname = DBLPVenue.getResearchDomainShortName(domainId);
		String insertSql1 = "INSERT INTO dblp_topic_"+shortname+ " (tid, keyword, istop) VALUES (?,?, ?)";
		String insertSql2 = "INSERT INTO dblp_doctopic_"+shortname+ " (id, tid, prob) VALUES (?,?, ?)";

		try {
			prepareForTopicImport(conn, shortname);
			BufferedReader docTopicIn = new BufferedReader(new FileReader(docTopicFile));
			BufferedReader topicKWIn = new BufferedReader(new FileReader(topicKWFile));
			PreparedStatement insst= conn.prepareStatement(insertSql1);
			String line;
			System.out.println("Inserting topic-keyword relation");
			while ((line = topicKWIn.readLine()) != null) {
				String[] fields= line.split("\\s+");
				String topicID= getTopicId(fields[0],minY,maxY, shortname);
				insst.setString(1, topicID);			
				for (int i=2; i < fields.length; i++) {
					insst.setString(2, fields[i].trim());	
					if (i <= TopicConstants.FIRSTFEWWORDS) 
						insst.setShort(3, (short)1);						
					else
						insst.setShort(3, (short)0);	
					insst.addBatch();
				}
			}
			insst.executeBatch();
			insst.close();
			topicKWIn.close();
			insst= conn.prepareStatement(insertSql2);

			// Now insert Doc-Topic Relationship
			Pattern pattern = Pattern.compile("\\d+");			
			int batchCount=0;
			System.out.println("Inserting topic-paper relation");
			while ((line = docTopicIn.readLine()) != null) {
				String[] fields= line.split("\\s+");
				Matcher matcher = pattern.matcher(fields[1]);
				if (matcher.find()==false) {
					System.out.println("Couldn't find paperID from: " + line);
					continue;
				}
				int docId = Integer.valueOf(matcher.group());
				for (int i=2, j=0;i < fields.length; i=i+2,j++) {
					if (j < TopicConstants.FIRSTFEWDOCS ) {
						String topicId= getTopicId(fields[i],minY,maxY, shortname);
						float currentTopicProbability= Float.valueOf(fields[i+1]);
						if (currentTopicProbability >= TopicConstants.DOCTOPICPROBTHR) {
							insst.setInt(1, docId);
							insst.setString(2, topicId);
							insst.setFloat(3, currentTopicProbability);
							insst.addBatch();
							batchCount++;
						}
					}
				}
				if (batchCount > 10000) {
					insst.executeBatch();
					batchCount=0;
				}
			}
			insst.executeBatch();
			insst.close();
			docTopicIn.close();
			
			System.out.println("Inserting author-keyword relation");
			createAuthKWRel(conn, domainId, minY, maxY);
			System.out.println("Inserting Keyword-author-Collaboration relation");
			createAuthCollabKWRel(conn, domainId, minY, maxY);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return count;

	}
	
	private void prepareForTopicImport(Connection conn, String shortname) throws SQLException {
		String crTopicTableSQL1 = DBLPSQLs.getTopicTableCreationSQL(shortname);
		String crTopicTableSQL2 = DBLPSQLs.getDocTopicTableCreationSQL(shortname);	
	
		String delTopicTableSQL1 = "TRUNCATE TABLE dblp_topic_" +shortname;
		String delTopicTableSQL2 = "TRUNCATE TABLE dblp_doctopic_" +shortname;

		Statement st=conn.createStatement();
		st.executeUpdate(crTopicTableSQL1);
		st.close();
		st=conn.createStatement();
		st.executeUpdate(crTopicTableSQL2);
		st.close();

		st=conn.createStatement();
		st.executeUpdate(delTopicTableSQL1);
		st.close();
		st=conn.createStatement();
		st.executeUpdate(delTopicTableSQL2);
		st.close();

	}
	
	private String createAuthKWRel(Connection conn, int domainId, int minY, int maxY) throws SQLException {
		String shortname = DBLPVenue.getResearchDomainShortName(domainId);
		String currResearchDomainDB = DBLPVenue.getResearchDomainViewName(domainId);
		String relTable = "dblp_authkw_" +shortname;
		// Authors grouped by keywords
		String authkwview= DBLPSQLs.getAuthorsGroupedByTopKW(currResearchDomainDB, 
				shortname, minY, maxY);

		String authRelTableCr = DBLPSQLs.getAuthKWRelTableCreationSQL(shortname);
		String delAuthRelTable = "TRUNCATE TABLE " + relTable; 
		Statement st=conn.createStatement();
		st.executeUpdate(authRelTableCr);
		st.close();
		st=conn.createStatement();
		st.executeUpdate(delAuthRelTable);
		st.close();
		String sql = "INSERT INTO " + relTable + " " + authkwview ;
		st=conn.createStatement();
		st.execute(sql);
		st.close();
		return relTable;
	}

	private String createAuthCollabKWRel(Connection conn, int domainId, int minY, int maxY) throws SQLException {
		String shortname = DBLPVenue.getResearchDomainShortName(domainId);
		String currResearchDomainDB = DBLPVenue.getResearchDomainViewName(domainId);

		String collabTableCr = DBLPSQLs.getAuthCollabOverKWTableCreationSQL(shortname);
		Statement st=conn.createStatement();
		st.executeUpdate(collabTableCr);
		st.close();
		String collabTable = "dblp_authcollabkw_" + shortname;
		String delTableSQL = "TRUNCATE TABLE " + collabTable; 
		st=conn.createStatement();
		st.executeUpdate(delTableSQL);
		st.close();
		System.out.println("Creating collaboration master");
		String collabMasterTable= createCollaborationPair(conn, currResearchDomainDB, shortname, minY, maxY);
		String relTableName = "dblp_authkw_" +shortname; 
		String selsql = "SELECT t1.keyword AS keyword, t1.author AS author, t2.author AS collabauth, rel.collabcnt AS collabcnt FROM " + 
		relTableName + " t1, " + relTableName + " t2, " + collabMasterTable + " rel " +
		" WHERE t1.keyword = t2.keyword AND t1.author = rel.author AND t2.author = rel.collabauth ORDER BY t1.keyword, t1.author";

		String sql = "INSERT INTO " + collabTable + " " + selsql ;
		st=conn.createStatement();
		st.execute(sql);
		st.close();
		return collabTable;
	}

	private String createCollaborationPair(Connection conn, String viewname, String shortname, int minY, int maxY) throws SQLException {
		String collabview= DBLPSQLs.getCollaborationRel(viewname, null,null, minY, maxY);
		String crCollabTableSQL = DBLPSQLs.getCollabRelTableCreationSQL(shortname);
		String collabTable= "dblp_collab_" +shortname;
		Statement st=conn.createStatement();
		st.executeUpdate(crCollabTableSQL);
		st.close();
		String delCollabTableSQL = "TRUNCATE TABLE " + collabTable;
		st=conn.createStatement();
		st.executeUpdate(delCollabTableSQL);
		st.close();
		
		String sql = "INSERT INTO " + collabTable + " " + collabview ;
		st=conn.createStatement();
		st.execute(sql);
		st.close();
		return collabTable;
	}

	

	@Deprecated
	public final int importTopicsOld(Connection conn, int domainId, int minY, int maxY, String docTopicFile, String topicKWFile) {
		int count=0;
		int topCount=4;
		String shortname = DBLPVenue.getResearchDomainShortName(domainId);
		String crTopicTableSQL1 = DBLPSQLs.getTopicTableCreationSQL(shortname);
		String crTopicTableSQL2 = DBLPSQLs.getDocTopicTableCreationSQL(shortname);	
	
		String delTopicTableSQL1 = "TRUNCATE TABLE dblp_topic_" +shortname;
		String delTopicTableSQL2 = "TRUNCATE TABLE dblp_doctopic_" +shortname;
		
		String insertSql1 = "INSERT INTO dblp_topic_"+shortname+ " (tid, topkw, otherkw) VALUES (?,?, ?)";
		String insertSql2 = "INSERT INTO dblp_doctopic_"+shortname+ " (id, tid, prob) VALUES (?,?, ?)";

		try {
			Statement st=conn.createStatement();
			st.executeUpdate(crTopicTableSQL1);
			st.close();
			st=conn.createStatement();
			st.executeUpdate(crTopicTableSQL2);
			st.close();
			
			st=conn.createStatement();
			st.executeUpdate(delTopicTableSQL1);
			st.close();
			st=conn.createStatement();
			st.executeUpdate(delTopicTableSQL2);
			st.close();

			BufferedReader docTopicIn = new BufferedReader(new FileReader(docTopicFile));
			BufferedReader topicKWIn = new BufferedReader(new FileReader(topicKWFile));
			PreparedStatement insst= conn.prepareStatement(insertSql1);
			String line;
			while ((line = topicKWIn.readLine()) != null) {
				String[] fields= line.split("\\s+");
				String topicID= getTopicId(fields[0],minY,maxY,shortname);
				StringBuffer topKW, otherKW;
				topKW= new StringBuffer();
				otherKW= new StringBuffer();
				for (int i=2; i < fields.length; i++) {
					if (i <= TopicConstants.FIRSTFEWWORDS) 
						topKW.append(fields[i] +" ");
					else
						otherKW.append(fields[i] +" ");
				}
				topKW.trimToSize();
				otherKW.trimToSize();
				insst.setString(1, topicID);
				insst.setString(2, topKW.toString());
				insst.setString(3, otherKW.toString());
				insst.addBatch();
			}
			insst.executeBatch();
			insst.close();
			topicKWIn.close();
			insst= conn.prepareStatement(insertSql2);

			// Now insert Doc-Topic Relationship
			Pattern pattern = Pattern.compile("\\d+");
			
			int batchCount=0;
			while ((line = docTopicIn.readLine()) != null) {
				String[] fields= line.split("\\s+");
				Matcher matcher = pattern.matcher(fields[1]);
				if (matcher.find()==false) {
					System.out.println("Couldn't find paperID from: " + line);
					continue;
				}
				int docId = Integer.valueOf(matcher.group());
				for (int i=2, j=0;i < fields.length; i=i+2,j++) {
					if (j < topCount ) {
						String topicId= getTopicId(fields[i],minY,maxY,shortname);
						float currentTopicProbability= Float.valueOf(fields[i+1]);
						if (currentTopicProbability >= 0.1) {
							insst.setInt(1, docId);
							insst.setString(2, topicId);
							insst.setFloat(3, currentTopicProbability);
							insst.addBatch();
							batchCount++;
						}
					}
				}
				if (batchCount > 10000) {
					insst.executeBatch();
					batchCount=0;
				}
			}
			insst.executeBatch();
			insst.close();
			docTopicIn.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return count;
	}
	/*
	public int createTextCorpusFromDB(boolean inMemory) {
		String sql = "SELECT ENTRYID, TITLE FROM ENTRY";
		Statement st;
		ResultSet rs;
		int count=0;
		try {
			st = _conn.createStatement();
			BibEntry anEntry= null;
			st = _conn.createStatement();
			boolean success = st.execute(sql);
			if (success) {
				String corpusBase= _appBase + File.separator + _inputCorpusDir;
				(new File(corpusBase)).mkdirs();
				rs = st.getResultSet();
				if (_bibEntries==null && inMemory==true)
					_bibEntries= new HashSet<BibEntry>();
				while (rs.next()) {
					anEntry= null;
					anEntry= new BibEntry();
					anEntry.setEntryId(rs.getInt(1));
					anEntry.setTitle(rs.getString(2));
					anEntry.setAbstract(rs.getString(3));
					if (inMemory == false)
						writeATextCorpus(corpusBase, anEntry);
					else {
						_bibEntries.add(anEntry);
					}

					count++;
				}
				rs.close();
			}
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return count;
	}
	 */
	private void writeTextIntoFile(String filename, String text) throws FileNotFoundException {
		PrintWriter writer = new PrintWriter(new FileOutputStream(filename));
		writer.println(text);
		writer.close();
	}
/*	private void writeATextCorpus(String dirName, BibEntry bib) {
		try {
			String filename= dirName + File.separator + bib.getEntryId() + ".txt";
			writeTextIntoFile(filename,bib.getTitleAsBOW());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} */
	private String getTopicId(String ndx, int minY, int maxY, String shortDomainName) {
//		String id = minY + "_" + maxY + "_" + ndx ;
		String id = shortDomainName + "_" + ndx ;

		return id;
	}
	/** 
	 * This stupid API recreates the same table in different format as the original table is screwed up.
	 * @param conn
	 * @param domainId
	 * @throws SQLException
	 */
	public int createCitationTable(Connection conn, int domainId) throws SQLException {
		String shortname = DBLPVenue.getResearchDomainShortName(domainId);
		String crSQL=DBLPSQLs.getCitationMasterCreationSQL(shortname);
		String delSQL = "TRUNCATE TABLE dblp_citation_" +shortname;
		String sql[] = DBLPSQLs.getRawCitationLoadSQL(shortname);
		String loadSQL=sql[0], inssql= sql[1];
	
		Statement st=conn.createStatement();
		st.executeUpdate(crSQL);
		st.close();
		st=conn.createStatement();
		st.executeUpdate(delSQL);
		st.close();
		
		st=conn.createStatement();
		ResultSet rs= st.executeQuery(loadSQL);
		PreparedStatement insst= conn.prepareStatement(inssql);
		int batchCount=0;
		int totalRec=0;
		while (rs.next()) {
			insst.setInt(1, rs.getInt(1));
			insst.setString(2, rs.getString(2));
			int year=0;
			if (rs.getString(3).equalsIgnoreCase("")== false)
				year= Integer.valueOf(rs.getString(3).trim());
			insst.setInt(3, year);
			insst.setString(4, rs.getString(4).trim());
			insst.addBatch();
			batchCount++;
			if (batchCount > 10000) {
				insst.executeBatch();
				batchCount=0;
			}
			totalRec++;
		}
		insst.executeBatch();
		insst.close();
		rs.close();
		return totalRec;
	}
}
