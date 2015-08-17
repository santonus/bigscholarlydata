package org.dblp.dbextractor;

public class DBLPVenue {
	public static final int SE =1;
	public static final int DB =2;
	public static final int AI = 3;
	public static final int IR = 4;
	public static final int OS = 5;

	public static String [] SEVENUE = {"tse", "software", "tosem", "jss", 
		"icse", "fse", "oopsla", "ase", "kbse", "cbse", "fase", "ecoop", "apsec", "issta",
		"issre", "wicsa"}; // for source_id column
	public static String [] SEVENUEEXTRA = {"SIGSOFT FSE", "ESEC/SIGSOFT FSE"}; // for source column
	public static String [] DBVENUE = {"edbt", "icde", "icdm", "icdt", 
		"icml", "kdd", "sdm", "pods", "sigmod", "vldb", "ecml", "pkdd", "wsdm", "dcc",
		"tkde", "dmkd", "tods"}; // for source_id column
	public static String [] AIVENUE = {"ai", "air", "amai", "alife", "ci", "cogsci", "connection", "ijar", "aim", "tfs", "jar", "jair", "jocn",
		"expert", "ras", "ker", "aai", "trob", "jcns", "jetai", "jiis", "aicom",
		"icml", "aaai","ijcai", "aamas", "ecai", "eai", "ictai", "nca", "neco", "nn", "kr", "icra", "uai", "eai","eml"};
	public static String [] OSVENUE = {"tocs", "tpds", "sigops", "cl", "ism", "tos", "linux", 
		"mkern","mach","osdi","sosp","asplos","usenix","lisa","hotos","sigops","eurosys","plos", "advcs"};
	public static String [] IRVENUE = {"tois","jasis","ipl","ipm","ir",
		"sigir","amr","trec","cikm","spire","jcdl","ecir","trecvid","inex","iral","drr","him","icadl","mira",
		"icws","icwe","wi","wism","iswc","cloudcom","www"};
	public static final String getResearchDomainViewName(int id) {
		switch(id) {
		case SE: return "dblp_pub_se";
		case DB: return "dblp_pub_db";
		case AI: return "dblp_pub_ai";
		case IR: return "dblp_pub_ir";
		case OS: return "dblp_pub_os";
		default: return null;
		}
	}
	public static final String getResearchDomainAbstractSQL(int id, int minY, int maxY) {
		String sql=null;
		switch(id) {
		/** LEFT JOIN should have been ideal. Not working now. No time to debug */
		case SE: sql= "SELECT p.id, p.title, abs.paperAbstract FROM " + getResearchDomainViewName(id) + 
		" AS p, dblp_pprdet AS abs WHERE " +
		"p.id = abs.refId AND p.year >= " + minY + " and p.year <= " + maxY ; break;
		case DB: sql= "SELECT p.id, p.title, abs.abstract_content FROM " + getResearchDomainViewName(id) + 
		" AS p LEFT JOIN dblp_pub_abstracts_db AS abs ON p.id = abs.id WHERE " +
		" p.year >= " + minY + " and p.year <= " + maxY ; break;
		case AI: sql= "SELECT p.id, p.title, abs.abstract_content FROM " + getResearchDomainViewName(id) + 
		" AS p LEFT JOIN dblp_pub_abstracts_ai AS abs ON p.id = abs.id  WHERE " +
		" p.year >= " + minY + " and p.year <= " + maxY ; break;
		case IR: sql= "SELECT p.id, p.title, abs.abstract_content FROM " + getResearchDomainViewName(id) + 
		" AS p LEFT JOIN dblp_pub_abstracts_ir AS abs ON p.id = abs.id  WHERE " +
		" p.year >= " + minY + " and p.year <= " + maxY ; break;
		case OS: sql= "SELECT p.id, p.title, abs.abstract_content FROM " + getResearchDomainViewName(id) + 
		" AS p LEFT JOIN dblp_pub_abstracts_os AS abs ON p.id = abs.id  WHERE " +
		" p.year >= " + minY + " and p.year <= " + maxY ; break;
		default: break;
		}
		return sql;
	}
	
	public static final String getResearchDomainShortName(int id) {
		switch(id) {
		case SE: return "SE";
		case DB: return "DB";
		case AI: return "AI";
		case IR: return "IR";
		case OS: return "OS";
		default: return null;
		}
	}

	public static final String getVenueIDAsSQL(int id) {
		String sql = null;
		StringBuffer venues= getVenuesAsWhereClause(id);
		if (venues != null) {
			String dbName = getResearchDomainViewName(id);
			sql=  "CREATE OR REPLACE VIEW " + dbName +  " AS SELECT * FROM dblp_pub_new WHERE " + venues.toString();
		}
		return sql;
	}

	public static StringBuffer getVenuesAsWhereClause(int id) {
		StringBuffer venues= null;
		switch(id) {
		case SE: 
			venues = getVenuesAsWhereClause(SEVENUE,SEVENUEEXTRA);
			break;
		case DB: 
			venues= getVenuesAsWhereClause(DBVENUE,null);
			break;
		case AI: 
			venues= getVenuesAsWhereClause(AIVENUE,null);
			break;
		case IR: 
			venues= getVenuesAsWhereClause(IRVENUE,null);
			break;
		case OS: 
			venues= getVenuesAsWhereClause(OSVENUE,null);
			break;
		default:
			break;
		}
		return venues;
	}
	private static StringBuffer getVenuesAsWhereClause(String[] venue,String[] venueExtra) {
		StringBuffer venues;
		venues= new StringBuffer("source_id like '" + venue[0] +"' ");
		for (int i=1; i < venue.length;i++) {
			venues.append("OR source_id like '" + venue[i] +"'" ); 
		}
		for (int i=0; venueExtra != null && i < venueExtra.length;i++) {
			venues.append("OR source like '" + venueExtra[i] +"'" ); 
		}
		return venues;
	}
}
