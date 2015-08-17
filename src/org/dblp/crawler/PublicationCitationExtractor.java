package org.dblp.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
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

import org.dblp.dbextractor.DBConstants;
import org.dblp.dbextractor.DBLPVenue;
import org.dblp.dbextractor.DataManager;
import org.dblp.crawler.model.TitleInfoVO;


/**
 * For each entry in the se view the citation count and the citation link(href) 
 * is stored in the mas_se_citation_reference_links table
 * @author Abhi
 *
 */
public class PublicationCitationExtractor {
	String URL_MAS = "http://academic.research.microsoft.com/Search?query=";
	String computerScienceFilter = "&searchtype=2&SearchDomain=2";
	ArrayList<TitleInfoVO> titleList = new ArrayList<TitleInfoVO>();
	File crawlLogFile  = new File(CrawlerConstants.OUTPUTDIRPATH+File.separator+
			CrawlerConstants.CRAWLRESDIR+File.separator+"crawl.log");
	private int viewType;
	
	DataManager mgr = new DataManager();
	PreparedStatement ps = null;
	Connection conn = null;
	
	public PublicationCitationExtractor(int viewType) {
		this.viewType  = viewType;
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
	public final void close() {
		mgr.closeConnection();
	}

	public void getPublicationInfoFromView() {

		int count = 0;
		String selectPublicationTitle = "SELECT title,id,year FROM DBLP_PUB_"+DBLPVenue.getResearchDomainShortName(viewType)+" ORDER BY id";
		Statement st;
		ResultSet rs;
		try {
			st = conn.createStatement();
			rs = st.executeQuery(selectPublicationTitle);
			while (rs.next()) {
				String title = rs.getString("title");
				if (title.endsWith(".")) {
					title = title.substring(0, title.length() - 1);
				}
				String id  = rs.getString("id");
				
				String year = rs.getString("year");
				titleList.add(new TitleInfoVO(title,id,year,null,null,null,DBLPVenue.getResearchDomainShortName(viewType)));
				count++;
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//mgr.closeConnection();
/*		for (Iterator iterator = titleList.iterator(); iterator.hasNext();) {
			TitleInfoVO titleInfoVO = (TitleInfoVO) iterator.next();
			System.out.println(titleInfoVO);
		}
		System.out.println(titleList.size());*/
	}

	public void parseAndStoreSearchResultsForTitleQuery(int startIndex) throws InterruptedException, IOException {
		int totalTitleCount = titleList.size();
		PrintWriter log = new PrintWriter(new FileWriter(crawlLogFile,true),true);
		
		
		log.println("Start Time................."+new Date());
		log.println("Total Title Count.........."+totalTitleCount);
		log.println();
		
		try {
			for (int titleCount = startIndex; titleCount < titleList.size(); titleCount++) {
								
				String title = ((TitleInfoVO)titleList.get(titleCount)).getTitle();
				String id = ((TitleInfoVO)titleList.get(titleCount)).getId();
				String year = ((TitleInfoVO)titleList.get(titleCount)).getYear();
				
				String fileName = CrawlerConstants.OUTPUTDIRPATH+File.separator+
				CrawlerConstants.CRAWLRESDIR+File.separator+		
					DBLPVenue.getResearchDomainShortName(viewType)+"/Result_"+ id +"_"+titleCount;
				File htmlFile= new File(fileName);
				if(htmlFile.exists()==false){
					//sleep for random time, so that you dont choke the server with requests
					Thread.sleep((long) (Math.random()*3109));
					log.println("Title..................."+title);
					log.println("Creating File.........."+"Crawled_Search_Results/"+DBLPVenue.getResearchDomainShortName(viewType)+"/Result_"+ id +"_"+titleCount);
					log.println();
					
					PrintWriter out = new PrintWriter(htmlFile);
					out.println("Queried Title = "+title);
					out.println("----------------------------------------------------------------------------------------------------------------------------");
					URL url = new URL(URL_MAS + URLEncoder.encode(title+" year="+year, "UTF-8")  + computerScienceFilter);
					
					String temp;
					BufferedReader reader = null;
					
					try{
						reader = new BufferedReader(new InputStreamReader(url.openStream()));
					}catch (IOException e) {
						log.println(e.getMessage());
						if(!htmlFile.delete()){
							log.println("Failed to delete the file"+htmlFile.toString());
						}
						log.println("Failed to open connection for index = "+titleCount+" title = "+title);
						log.println("Retrying to open..............");
						parseAndStoreSearchResultsForTitleQuery(titleCount);
					}
					
					while (null != (temp = reader.readLine())) {
						out.println(temp);
					}
	
					out.close();
					log.flush();
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		log.println();
		log.println("End Time................."+new Date());
		log.close();
			
	}

	
	
	public void parseAndStoreLinksForCitations() throws SQLException {
		int flag = 0;
		String masPubID = "";
		
		for (int titleCount = 0; titleCount <titleList.size(); titleCount++) {
			TitleInfoVO titleInfo = (TitleInfoVO) titleList.get(titleCount);

			String title = ((TitleInfoVO) titleList.get(titleCount)).getTitle();
			String id = ((TitleInfoVO) titleList.get(titleCount)).getId();
			String fileName = CrawlerConstants.OUTPUTDIRPATH+File.separator+
			CrawlerConstants.CRAWLRESDIR+File.separator+
				DBLPVenue.getResearchDomainShortName(viewType)+"/Result_" + id + "_"+ titleCount;
			String citationURL="";
			int citationCount = 0;
			
			File inputFile = new File(fileName);
			if (inputFile.exists()) {
				Document doc=null;
				try {
					
					doc = Jsoup.parse(inputFile, "UTF-8");

					// Elements links = doc.getElementsByTag("a");
					System.out.println("In title[" + titleCount + "]=" + title);
					Elements links = doc.select("a[href]");
					for (Element link : links) {
						String linkHref = link.attr("href");
						String linkText = link.text();
						
						
						if (linkText.equalsIgnoreCase(title)) {
							System.out.println("link href= " + linkHref
									+ " linkText = " + linkText);
							flag = 1;
							//store the id of this publication to compare against the citationLink
							String[] hrefArr = linkHref.split("/");
							masPubID = hrefArr[1]; 
							System.out.println("masPubID = "+masPubID);
						}
						if (flag == 1 && linkText.contains("Citation") && linkHref.endsWith("id="+masPubID)) {
							 System.out.println("link href= " + linkHref +
							 " linkText = " + linkText);
							flag = 0;
							citationURL = "http://academic.research.microsoft.com/"
									+ linkHref;
							
							linkText = linkText.replaceAll(" ", "");
							String citationText[] = linkText.split(":");
							if (citationText.length == 2) {
								citationCount = Integer
										.parseInt(citationText[1]);
							}

							System.out.println("Citation URL = " + citationURL);
							System.out.println("Citation count = "
									+ citationCount);


							break;
						}

					}
					insertIntoCitationReferenceTable(titleInfo,	citationURL, citationCount);
					doc = null;//set doc to null to avoid outof memory exception
					System.out.println();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	
	private void insertIntoCitationReferenceTable(TitleInfoVO titleInfo,String citationURL, int citationCount) throws SQLException {

		int id = Integer.parseInt(titleInfo.getId());
		String title = titleInfo.getTitle();
		if (ps == null) {
			String insertCitationCountAndLink = "insert ignore into mas_"
					+ DBLPVenue.getResearchDomainShortName(viewType)
					+ "_citation_reference_links(id,publication_title,citation_count,citation_link) "
					+ "values (?,?,?,?)";
			ps = conn.prepareStatement(insertCitationCountAndLink);
		}
		ps.setInt(1, id);
		ps.setString(2, title);
		ps.setInt(3, citationCount);
		ps.setString(4,citationURL);
			
		ps.executeUpdate();
		//ps.close();
		//mgr.closeConnection();

	}
	
 
/*
	//Clean up exception handling!!
	public static void main(String[] args) throws InterruptedException, SQLException, IOException{
		PublicationCitationExtractor citationExtractor = new PublicationCitationExtractor(DBLPVenue.AI);
		try{
			citationExtractor.getPublicationInfoFromView();
			citationExtractor.parseAndStoreSearchResultsForTitleQuery(0);
			System.out.println("Storing Citation links into DB... ");
			citationExtractor.parseAndStoreLinksForCitations();
		}finally{
			citationExtractor.mgr.closeConnection();
			//citationExtractor.ps.close();
		}
	}
*/
	public ArrayList<TitleInfoVO> getTitleList() {
		return titleList;
	}

	public void setTitleList(ArrayList<TitleInfoVO> titleList) {
		this.titleList = titleList;
	}

}
