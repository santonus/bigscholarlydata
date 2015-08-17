package org.dblp.crawler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;

import org.dblp.dbextractor.DBConstants;
import org.dblp.dbextractor.DBLPVenue;
import org.dblp.dbextractor.DataManager;
import org.dblp.crawler.model.CitationLinkVO;



/**
 * @author Abhi
 * Gets the link for every entry in the MAS_XX_CITATION_REFERENCE_LINKS table 
 * ,crawls through them and stores citation details in the mas_XX_pub_citations table  
 */
public class CitationDetailUploader {
	static PrintWriter log;
	int viewType;
	ArrayList<CitationLinkVO> citationLinkVOList=null;

	DataManager mgr = new DataManager();
	Connection conn = null;

	
	public CitationDetailUploader(int viewType) {
		this.viewType = viewType;
		mgr.connectmySql(DBConstants.DB_NAME);
		conn = mgr.getConnection();
		if(CrawlerConstants.HTTP_PROXY!=null && CrawlerConstants.HTTP_PROXY.length()>0){
			System.setProperty("http.proxyHost", CrawlerConstants.HTTP_PROXY);
			System.setProperty("http.proxyPort", CrawlerConstants.HTTP_PORT);
			System.setProperty("http.proxyUser", CrawlerConstants.HTTP_USER);
			System.setProperty("http.proxyPassword", CrawlerConstants.HTTP_PWD);
			System.setProperty("http.proxyAuth", CrawlerConstants.HTTP_AUTH);
		}

	}

	private  void populateCitationDetails(int startIndex)  {
		citationLinkVOList = getUniqueCitations();
		
		int START_INDEX = startIndex;
		int END_INDEX = citationLinkVOList.size();
		
		System.out.println("Final List Size = "+citationLinkVOList.size());
		log.println("Final List Size = "+citationLinkVOList.size());
		
		for (int i = START_INDEX; i < END_INDEX; i++) {
			System.out.println("CURRENT_INDEX_________________________________ "+i);
			log.println("CURRENT_INDEX_________________________________ "+i);
			CitationLinkVO citationLinkVO = citationLinkVOList.get(i);
			System.out.println(citationLinkVO.getPublicationId());
			System.out.println(citationLinkVO.getPublicationTitle());
			System.out.println(citationLinkVO.getCitationLink());
			System.out.println(citationLinkVO.getCitationCount());
			try {
				crawlAndInsertCitationDetails(i,citationLinkVO);
			} catch (IOException e) {
				e.printStackTrace();
				
				log.println("Connection Error Occurred at index  = "+i);
				System.out.println("Connection Error Occurred at index  = "+i);
				log.println("Retrying after 5 mins....");
				System.out.println("Retrying after 5 mins ....");

				try {
					Thread.sleep(30000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				--i;
				
			}
			
		}
	}

	private  ArrayList<CitationLinkVO> getUniqueCitations() {
		ArrayList<String> duplicatePublicationTitles = getDuplicatePublicationTitles();
		System.out.println("duplicatePublicationTitles = "
				+ duplicatePublicationTitles.size());
		log.println("duplicatePublicationTitles = "	+ duplicatePublicationTitles.size());
		ArrayList<CitationLinkVO> citationLinkVOList = getCitationLinksForPublication();
		System.out.println("citationLinkVOList = " + citationLinkVOList.size());
		log.println("citationLinkVOList = " + citationLinkVOList.size());
		ArrayList<CitationLinkVO> returnCitationLinkVOList = new ArrayList<CitationLinkVO>();

		for (Iterator iterator = citationLinkVOList.iterator(); iterator
				.hasNext();) {
			CitationLinkVO citationLinkVO = (CitationLinkVO) iterator.next();
			boolean toAdd = true;
			for (String pubTitle : duplicatePublicationTitles) {
				if (pubTitle.equalsIgnoreCase(citationLinkVO
						.getPublicationTitle())) {
					toAdd = false;
				}
			}
			if (toAdd)
				returnCitationLinkVOList.add(citationLinkVO);
		}

		return returnCitationLinkVOList;
	}

	public ArrayList<String> getDuplicatePublicationTitles() {
		ArrayList<String> duplicatePublicationTitles = new ArrayList<String>();

		Statement st;
		ResultSet rs;

		String queryOne = "select publication_title from MAS_"+DBLPVenue.getResearchDomainShortName(viewType)+"_CITATION_REFERENCE_LINKS GROUP BY publication_title HAVING COUNT(1) > 1";
		try {
			st = conn.createStatement();
			rs = st.executeQuery(queryOne);
			while (rs.next()) {
				duplicatePublicationTitles.add(rs
						.getString("publication_title"));
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}

	private  void crawlAndInsertCitationDetails(
			int currentIndex, CitationLinkVO citationLinkVO) throws IOException {

		String url = citationLinkVO.getCitationLink();
		double citationCount = citationLinkVO.getCitationCount();
		
		log.println(".........................Citation Count for \""+citationLinkVO.getPublicationTitle()+"\" = "+citationCount+"....................");
		
		String startString = "&start=";
		String endString = "&end=";
		double increment = 70.0;
		double citationPages = Math.ceil(citationCount / increment);

		int count = 0;
		int startIndex = 1;
		int endIndex = (int) increment;
		for (int page = 0; page < citationPages; page++, startIndex = (int) (startIndex + increment), endIndex = (int) (endIndex + increment)) {
			try {
				Thread.sleep((long) (Math.random()*30000));
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			int pubId = citationLinkVO.getPublicationId();
			String publicationTitle = citationLinkVO.getPublicationTitle();
			String citationTitle = "";
			String citationYearOfPub = "";

			ArrayList<String> authorList = new ArrayList<String>();

			String citationURL = url + startString + startIndex + endString
					+ endIndex;
			System.out.println();
			log.println();
			System.out.println("New URL = " + citationURL);
			log.println("New URL = " + citationURL);
			Document doc = Jsoup.connect(citationURL).timeout(0).get();
			Elements links = doc.select("li[class]");
			print("\nLinks: (%d)", links.size());

			for (Element link : links) {
				System.out.println("Current Index= " + ++count);
				citationTitle = "";
				citationYearOfPub = "";
				// System.out.println("li text = " + link.text());
				Elements childrenLinks = link.children();
				for (Element child : childrenLinks) {
					// get title for element
					Elements titleElements = child
							.getElementsByAttributeValueEnding("id", "_Title");
					if (titleElements.size() != 0) {
						for (Element titleElement : titleElements) {
							if ("".equals(titleElement.text()) == false)
								// System.out.println("Title Element= "+titleElement.text());
								citationTitle = titleElement.text().trim();
							// System.out.println("Title Link = "+titleElement.attr("href"));
						}
					}

					Elements authorElements = child
							.getElementsByClass("author-name-tooltip");
					// System.out.println("confElements.size = "+confElements.size());
					if (authorElements.size() != 0) {
						authorList = new ArrayList<String>();
						for (Element authorElement : authorElements) {
							if ("".equals(authorElement.text()) == false) {
								System.out.print(authorElement.text() + ",");
								authorList.add(authorElement.text().trim());
							}
						}
					}
					//System.out.println();
					Elements confElements = child
							.getElementsByClass("conference");
					// System.out.println("confElements.size = "+confElements.size());
					if (confElements.size() != 0) {
						for (Element confElement : confElements) {

							Elements yearsElement = confElement
									.getElementsByClass("year");
							for (Element yearElement : yearsElement) {
								if ("".equals(yearElement.text()) == false) {
									String[] year = yearElement.text().split(
											",");
									citationYearOfPub = year[year.length - 1]
											.trim();
								}
							}
							if ("".equals(citationYearOfPub)) {
								if (confElement.text().contains("Published in")) {
									String[] pubInfo = confElement.text()
											.split(" ");
									if (pubInfo.length == 3)
										citationYearOfPub = pubInfo[2].replace(
												".", "");
								}

							}
							if ("".equals(citationYearOfPub) == false) {
								// System.out.println("Year Element= "+yearOfPub);
							}
							if ("".equals(confElement.text()) == false)
								System.out.println("Conf Element= "
										+ confElement.text());
						}
					}

				}

				System.out.println("Information to be stored : ");
				System.out.println("pubId........................"+ pubId);
				System.out.println("publicationTitle............."+ publicationTitle);
				System.out.println("citationTitle................"+ citationTitle);
				System.out.println("citationYearOfPub............"+ citationYearOfPub);
				
				log.println("Information to be stored : ");
				log.println("pubId........................"+ pubId);
				log.println("publicationTitle............."+ publicationTitle);
				log.println("citationTitle................"+ citationTitle);
				log.println("citationYearOfPub............"+ citationYearOfPub);

				String authors = "";
				if(authorList!=null){
					System.out.println("Authors......................"+ authorList.toString().replace("[", "").replace("]", ""));
					log.println("Authors......................"+ authorList.toString().replace("[", "").replace("]", ""));
					authors = authorList.toString().replace("[", "").replace("]", "");
				}
				authorList = new ArrayList<String>();
				
				try {
					insertIntoCitationInfoTable(currentIndex,pubId,publicationTitle,citationTitle,citationYearOfPub,authors);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
		System.out.println("------------------------------------------------------------------------------------------------------------------------------");
		log.println("------------------------------------------------------------------------------------------------------------------------------");
		log.flush();
	}

	private  void insertIntoCitationInfoTable(int currentIndex,
			int pubId, String publicationTitle, String citationTitle,
			String citationYearOfPub, String authorList) throws SQLException {
			String insertCitationDetail = "INSERT INTO " +
					"mas_"+DBLPVenue.getResearchDomainShortName(viewType)+"_pub_citations(pub_id, pub_title, citation_title, citation_title_year, citation_authors) " +
					"VALUES(?,?,?,?,?)"; 

			PreparedStatement ps = conn.prepareStatement(insertCitationDetail);
			
			ps.setInt(1, pubId);
			ps.setString(2, publicationTitle);
			ps.setString(3,citationTitle);
			ps.setString(4,citationYearOfPub);
			ps.setString(5,authorList);
			
			ps.executeUpdate();
			
			ps.close();

	}

	public  ArrayList<CitationLinkVO> getCitationLinksForPublication() {
		ArrayList<CitationLinkVO> citationLinkVOList = new ArrayList<CitationLinkVO>();
		String selectCitationLink = "SELECT id,publication_title,citation_count,citation_link FROM MAS_"+DBLPVenue.getResearchDomainShortName(viewType)+"_CITATION_REFERENCE_LINKS where citation_count!=0";
		Statement st;
		ResultSet rs;
		try {
			st = conn.createStatement();
			rs = st.executeQuery(selectCitationLink);
			while (rs.next()) {
				int id = rs.getInt("id");
				String publicationTitle = rs.getString("publication_title");
				int citationCount = rs.getInt("citation_count");
				String citationLink = rs.getString("citation_link");

				CitationLinkVO citationLinkVO = new CitationLinkVO(
						id, publicationTitle, citationCount, citationLink);
				citationLinkVOList.add(citationLinkVO);

			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return citationLinkVOList;
	}

	private static void print(String msg, Object... args) {
		System.out.println(String.format(msg, args));
	}

	
	public static void main(String[] args) throws IOException {
		int viewType = DBLPVenue.AI;
		FileWriter fstream = new FileWriter(CrawlerConstants.OUTPUTDIRPATH+File.separator+
				CrawlerConstants.CRAWLRESDIR+File.separator+				
				"Citation_"+DBLPVenue.getResearchDomainShortName(viewType)+"_details.Log",true);
		CitationDetailUploader citationDetailUploader = new CitationDetailUploader(viewType);
		log = new PrintWriter(fstream,true);
		try{
			log.println("Current Date........"+new Date());
			citationDetailUploader.populateCitationDetails(0);
			log.close();
		}finally{
			citationDetailUploader.mgr.closeConnection();
		}
	}
}
