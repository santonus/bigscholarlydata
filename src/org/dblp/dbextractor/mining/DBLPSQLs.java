package org.dblp.dbextractor.mining;

public class DBLPSQLs {
	
	public static final String getSummary(String viewname) {
		String sql = "select source, source_id, count(title) as numpap, count(distinct author)" + 
		"as numauth, min(year) as inception from " + viewname + " p, dblp_author_ref_new a " +
		"where p.id=a.id group by source_id,source";
		return sql;
	}
	public static final String getAuthPubCountInTimeSlice(String viewname, int start, int end) {
		String sql = "select numpub, count(numpub) as authcnt from " +
		"(select a.author, count(a.author) as numpub " +
		"from dblp_author_ref_new as a, " + viewname + " pse where a.id = pse.id AND " +
		"pse.year >= " + start + " AND pse.year <= " + end +
		" group by a.author) " +
		"as filterlist group by numpub order by numpub";
		return sql;
	}
	
	public static final String getDistinctAuthInTimeSlice(String viewname, int start, int end) {
		String sql = "SELECT DISTINCT author from dblp_author_ref_new as a, " + viewname + " as p " +
		"where a.id = p.id and p.year >= " + start + " and p.year <= " + end;
		return sql;
	}
	// For Query 4
	public static final String getAuthorDetails(String viewname, int start, int end) {
		String sql = "SELECT author, COUNT(author) as pcount, MIN(year) as miny, MAX(year) as maxy, " +
		"COUNT(distinct source_id) as venuecnt FROM dblp_author_ref_new as a, " + viewname + " as p " +
		"WHERE a.id = p.id AND p.year >= " + start + " and p.year <= " + end + " group by author ";
		return sql;
	}
	public static final String getCoAuthoredPaperCount(String viewname, int start, int end) {
		String coauthPaperCnt= "SELECT a.author as author, count(pcoauth.pid) as coauthpcnt FROM dblp_author_ref_new as a, "+
		" (SELECT atmp.id as pid, count(*) as coauth FROM dblp_author_ref_new as atmp, " + viewname + " as p " +
		"WHERE atmp.id = p.id AND p.year >= " + start + " and p.year <= " + end + " GROUP BY atmp.id) as pcoauth " +
		" WHERE a.id = pcoauth.pid AND pcoauth.coauth > 1 GROUP BY a.author";
		return coauthPaperCnt;
	}
	public static final String getDistinctAuthCount(String viewname, int start, int end) {
		String distinctCoAuth= "SELECT a.author as author, COUNT(distinct coa.author)-1 as distauthcnt FROM " +
		"dblp_author_ref_new a, dblp_author_ref_new coa, " + viewname + " as p " +
		"WHERE a.id = coa.id AND a.id = p.id AND p.year >= " + start + " and p.year <= " + end + " GROUP BY a.author";
		return distinctCoAuth;
	}
	
	/**
	 * @param mainauth - The specific author, for which we are looking its collaborating authors. If null or "", it means for all authors
	 * @param collabAuth - The specific collaborating author who has collaborated with main author. If null, look for all collaborators
	 * @param start - start of time period
	 * @param end - end of time period
	 * @return the SQL string that returns author, collabAuthor, collabCount
	 * Note that mainauth = null and collabAuth not null- does not make a valid query
	 */
	public static final String getCollaborationRel(String viewname, String mainauth, String collabAuth, int start, int end) {
		String part1= "SELECT a.author as author, coa.author as collabauth, COUNT(coa.author) as collabcnt FROM " +
				"dblp_author_ref_new a, dblp_author_ref_new coa, "+ viewname + " as p " +
				"WHERE a.id = coa.id AND a.id = p.id AND a.author != coa.author AND p.year >= " + start + " AND p.year <= " + end;
		String part2="";
		if (mainauth != null && !mainauth.equals("")) {
			String str= " AND a.author like \""+mainauth+"\"";
			if (collabAuth != null && !collabAuth.equals(""))
				str += " AND coa.author like \""+collabAuth+"\"";
			part2=str;
		}
		String coauthRelSQL= part1 + part2 + " GROUP BY a.author, coa.author";
		return coauthRelSQL;
	}
	public static final String getCollabRelTableCreationSQL(String domainName) {
		String crCollabTableSQL = "CREATE TABLE IF NOT EXISTS dblp_collab_" +domainName +
		"(author varchar(70) NOT NULL, " +
		" collabauth varchar(70) NOT NULL, " + 
		" collabcnt int NOT NULL, " +
		" PRIMARY KEY (author,collabauth)) ";
		return crCollabTableSQL;
	}
	public static final String getOldAuthorsInTimeSlice(String viewname, int start, int end) {
		String sql = "create table tmpt select ca from " +
				"(select distinct author as ca from dblp_author_ref_new a, "+ viewname + " as p " +
				"where a.id = p.id and p.year >= " + start + " and p.year <= " + end + ") as currauths, " +
				"(select distinct author as pa from dblp_author_ref_new a, "+ viewname + " as p " +
				"where a.id = p.id and  p.year <= " + (start -1) +") as pastauths " +
				"where currauths.ca = pastauths.pa";
		return sql;
	}
	public static final String getOldOldCollaboration(String viewname, int start, int end) {
		String part1 = "SELECT a.author as author, coa.author as collabauth, COUNT(*) as collabcnt " +
				"FROM dblp_author_ref_new a, dblp_author_ref_new coa, tmpt t1, tmpt t2, " + viewname + " as p " +
				"WHERE a.id = coa.id AND a.id = p.id AND a.author != coa.author AND a.author = t1.ca " +
				"AND coa.author = t2.ca AND p.year >= " + start + " AND p.year <= " + end ;
		
		String coauthRelSQL= part1 + " GROUP BY a.author, coa.author";
		return coauthRelSQL;
	}
	/* Following methods deal with topic related queries
	 * 
	 * 	 
	 */
	// Stores a topic, and associated keywords. 
	public static final String getTopicTableCreationSQL(String shortname) {
		String crTopicTableSQL = "CREATE TABLE IF NOT EXISTS dblp_topic_" +shortname +
		"(tid varchar(50) NOT NULL, " +
		" keyword varchar(255) NOT NULL, " + 
		" istop tinyint UNSIGNED ZEROFILL, " + // Is this keyword a top keyword for this topic
		" PRIMARY KEY (tid,keyword)) ";
		return crTopicTableSQL;
	}
	/* Stores a grouping between a keyword and an author. If a topic contains a keyword w, and if this
	 * topic is linked to a paper p, written by a co-author a, we insert a record <w,a>.
	 * Keywords are fine-grained, one can do the same with topic also.
	 */
	
	public static final String getAuthKWRelTableCreationSQL(String shortname) {
		String crTopicTableSQL = "CREATE TABLE IF NOT EXISTS dblp_authkw_" +shortname +
		"(author varchar(70) NOT NULL, " +
		" keyword varchar(255) NOT NULL, " + 
		" PRIMARY KEY (keyword,author)) ";
		return crTopicTableSQL;
	}
	/* In the previous table, you can get a set of authors Aw who have written papers that contain a keyword
	 * w. But how many of them have really collaborated? This is captured in this table. A record
	 * <w, a, coa, nn> means that for a keyword w, there exists a paper which is co-authored by a and coa.
	 * This table is created by joining dblp_collab table with dblp_authkw table.
	 */
	public static final String getAuthCollabOverKWTableCreationSQL(String shortname) {
		String crTopicTableSQL = "CREATE TABLE IF NOT EXISTS dblp_authcollabkw_" +shortname +
		"(keyword varchar(255) NOT NULL, " + 
		"author varchar(70) NOT NULL, " + 
		" collabauth varchar(70) NOT NULL, " +
		"collabcnt int NOT NULL" +
		" ) ";
		return crTopicTableSQL;
	}

	@Deprecated
	public static final String getTopicTableCreationSQLOld(String domainName) {
		String crTopicTableSQL = "CREATE TABLE IF NOT EXISTS dblp_topic_" +domainName +
		"(tid varchar(50) NOT NULL, " +
		" topkw varchar(255) NOT NULL, " + 
		" otherkw varchar(255), " +
		" PRIMARY KEY (tid)) ";
		return crTopicTableSQL;
	}
	public static final String getDocTopicTableCreationSQL(String domainName) {
		String crTopicTableSQL = "CREATE TABLE IF NOT EXISTS dblp_doctopic_" +domainName +
		"(id int(11) NOT NULL, " +
		" tid varchar(50) NOT NULL, " + 
		" prob float NOT NULL, " +
		" PRIMARY KEY (id,tid)) ";
		return crTopicTableSQL;
	}
	public static final String getAbstractTableCreationSQL(String domainName) {
		String crTopicTableSQL = "CREATE TABLE IF NOT EXISTS dblp_pub_abstracts_" +domainName +
		"(id int(8) NOT NULL COMMENT 'Maps to the id field db_pub_xxxx view', " +
		" redirected_url varchar(200) NOT NULL COMMENT 'Redirected URL from the ee field in the db_pub_xxxx view', " +
		"  abstract_content longtext COMMENT 'Abstract of this paper', " + 
		"  PRIMARY KEY (id)) ";
		return crTopicTableSQL;
	}
	public static final String getRawCitationRefTableCreationSQL(String domainName) {
		String crTopicTableSQL = "CREATE TABLE IF NOT EXISTS mas_" +domainName + "_citation_reference_links " +
		"(id int(8) NOT NULL COMMENT 'Our internal database key in dblp_pub_new', " +
		" publication_title longtext NOT NULL, " + 
		" citation_count int(10) NOT NULL DEFAULT '0', " +
		"citation_link varchar(300) DEFAULT NULL, " +
		" PRIMARY KEY (id)) ";
		return crTopicTableSQL;
	}
	public static final String getRawCitationTableCreationSQL(String domainName) {
		String crTopicTableSQL = "CREATE TABLE IF NOT EXISTS mas_" +domainName + "_pub_citations " +
		" ( citation_id int(8) NOT NULL AUTO_INCREMENT COMMENT 'Citation ID and primary Key', " + 
		"  pub_id int(8) NOT NULL COMMENT 'ID of the publications refers to the key in dblp_pub_new', " +
		" pub_title longtext NOT NULL COMMENT 'Publication title whose citations are to be listed', " +
		" citation_title longtext NOT NULL COMMENT 'This paper has cited pub_title publication', " +
		" citation_title_year varchar(4) DEFAULT NULL COMMENT 'Year of publication of the Citation Title paper', " +
		" citation_authors longtext NOT NULL COMMENT 'Comma separated Authors in the citation paper', " +
		" PRIMARY KEY (citation_title(100),citation_id), " +
		" KEY `Index 2` (citation_id)	)";	
		return crTopicTableSQL;
	}
	
	
	public static final String getCitationMasterCreationSQL(String domainName) {
		String crCiteTableSQL = "CREATE TABLE IF NOT EXISTS dblp_citation_" +domainName +
		"(id int(8) NOT NULL, " +
		" citation_title longtext NOT NULL, " + 
		" citation_year int(4) unsigned, " +
		" citation_authors longtext, " +
		" INDEX(id)) ";
		return crCiteTableSQL;

	}
	public static final String[] getRawCitationLoadSQL(String domainName) {
		String [] res= new String[2];
		res[0] = "SELECT pub_id, citation_title, citation_title_year, citation_authors"  +
		" FROM mas_" + domainName + "_pub_citations ";
		res[1] = "INSERT INTO dblp_citation_"+domainName+ 
		" (id, citation_title, citation_year, citation_authors) VALUES (?,?,?,?)";
		return res;
	}

	public static final String getTopicDistributionByPaperCount(String domainName, String shortname, int start, int end) {
		String pubrangeview = "(SELECT id FROM " + domainName + " WHERE year >= " + start + " AND year <=" + end + ")"; 
		String topicPaperCount = "SELECT td.tid as tid, COUNT(p.id) as pcount FROM dblp_doctopic_" + shortname + " AS td LEFT JOIN " +
		pubrangeview + " AS p ON td.id = p.id GROUP BY td.tid";
/*		
		String topicPaperCount = "SELECT td.tid, COUNT(p.id) as pcount FROM dblp_doctopic_" + shortname + " AS td LEFT JOIN " +
		pubrangeview + " AS p ON td.id = p.id, dblp_topic_" + shortname + 
		" AS t WHERE t.tid = td.tid GROUP BY td.tid";
*/

		String sql = "SELECT tp.tid, t.keyword, tp.pcount FROM (" + topicPaperCount + ") AS tp, dblp_topic_"+shortname+ 
		" AS t WHERE t.tid = tp.tid AND t.istop = 1 order by tp.tid";
		return sql;
	}
	public static final String getTopicDistributionByCitationOld(String domainName, String shortname, int start, int end) {
		String pubrangeview = "(SELECT p.id as id, count(*) as citationCount FROM dblp_ppr_cite c, " + domainName + 
		" p WHERE p.id = c.paperid AND c.citationPaperId != -1 AND p.year >= " + start + " AND p.year <=" + end +
		" GROUP BY p.id )" ;
		String topicCiteCount = "SELECT td.tid as tid, SUM(pc.citationCount) as citationcount FROM dblp_doctopic_" + shortname + 
		" AS td LEFT JOIN " + pubrangeview + " AS pc ON td.id = pc.id GROUP BY td.tid";

		String sql = "SELECT tp.tid, t.keyword, citationcount FROM (" + topicCiteCount + ") AS tp, dblp_topic_"+shortname+ 
		" AS t WHERE t.tid = tp.tid AND t.istop = 1 order by tp.tid";
		return sql;
	}
	public static final String getTopicDistributionByCitation(String domainName, String shortname, int start, int end) {
		String pubrangeview = "(SELECT p.id as id, count(*) as citationCount FROM mas_se_pub_citations c, " + domainName + 
		" p WHERE p.id = c.pub_id AND c.citation_title_year != '' AND p.year >= " + start + " AND p.year <=" + end +
		" GROUP BY p.id )" ;
		String topicCiteCount = "SELECT td.tid as tid, SUM(pc.citationCount) as citationcount FROM dblp_doctopic_" + shortname + 
		" AS td LEFT JOIN " + pubrangeview + " AS pc ON td.id = pc.id GROUP BY td.tid";

		String sql = "SELECT tp.tid, t.keyword, citationcount FROM (" + topicCiteCount + ") AS tp, dblp_topic_"+shortname+ 
		" AS t WHERE t.tid = tp.tid AND t.istop = 1 order by tp.tid";
		return sql;
	}
	
	public static final String getTopicDistributionByCitationVariation(String domainName, String shortname, int start, int end) {
		String pubrangeview = "(SELECT c.id as id, count(*) as citationCount FROM dblp_citation_" + shortname +
		" c WHERE c.citation_year >= " + start + " AND c.citation_year <=" + end +
		" GROUP BY c.id )" ;
		String topicCiteCount = "SELECT td.tid as tid, SUM(pc.citationCount) as citationcount FROM dblp_doctopic_" + shortname + 
		" AS td LEFT JOIN " + pubrangeview + " AS pc ON td.id = pc.id GROUP BY td.tid";

		String sql = "SELECT tp.tid, t.keyword, citationcount FROM (" + topicCiteCount + ") AS tp, dblp_topic_"+shortname+ 
		" AS t WHERE t.tid = tp.tid AND t.istop = 1 order by tp.tid";
		return sql;
	}

	
	public static final String getAuthorsGroupedByTopKW(String domainName, String shortname, int start, int end) {
		String sql= "SELECT DISTINCT a.author, t.keyword FROM dblp_author_ref_new a, dblp_topic_"+shortname+" t, dblp_doctopic_"+shortname+" td," +
		domainName + " p " + 
		" WHERE a.id = td.id AND td.tid = t.tid AND a.id = p.id AND p.year >= " + start + " AND p.year <=" + end  +
		" AND t.istop = 1 ORDER BY t.keyword, a.author";
		return sql;
	}
	
	public static final String getAuthorTopicAffinityDistribution(String domainName, String shortname, int start, int end) {
		String sql= "SELECT a.author as author, td.tid as tid, SUM(td.prob) as affinity FROM dblp_author_ref_new a, dblp_doctopic_"+shortname+" td," +
		domainName + " p " + 
		" WHERE p.id = td.id AND a.id = p.id AND p.year >= " + start + " AND p.year <=" + end  +
		" GROUP BY a.author, td.tid ORDER BY author,tid";
		return sql;
	}
}

