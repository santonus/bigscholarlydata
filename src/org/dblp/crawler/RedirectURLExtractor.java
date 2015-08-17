package org.dblp.crawler;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;

import org.dblp.dbextractor.DBConstants;
import org.dblp.dbextractor.DBLPVenue;
import org.dblp.dbextractor.DataManager;
import org.dblp.crawler.model.TitleInfoVO;

public class RedirectURLExtractor {
	WebDriver driver =new HtmlUnitDriver();
	int viewType;
	public RedirectURLExtractor(int vT){
		if(CrawlerConstants.HTTP_PROXY!=null && CrawlerConstants.HTTP_PROXY.length()>0){
		System.setProperty("http.proxyHost", CrawlerConstants.HTTP_PROXY);
		System.setProperty("http.proxyPort", CrawlerConstants.HTTP_PORT);
		System.setProperty("http.proxyUser", CrawlerConstants.HTTP_USER);
		System.setProperty("http.proxyPassword", CrawlerConstants.HTTP_PWD);
		System.setProperty("http.proxyAuth", CrawlerConstants.HTTP_AUTH);
		((HtmlUnitDriver) driver).setProxy(CrawlerConstants.HTTP_PROXY, Integer.parseInt(CrawlerConstants.HTTP_PORT));
		}
		viewType=vT;
	}
	/*
	public static void main(String[] args) throws SQLException {
		RedirectURLExtractor redirectURLExtractor = new RedirectURLExtractor();
		int startIndex = 0;
		//redirectURLExtractor.storeRedirectedURL(DBLPVenue.DB,startIndex);
		//startIndex = 6806;
		redirectURLExtractor.storeRedirectedURL(DBLPVenue.AI,startIndex);
		
		
	}*/

	public void storeRedirectedURL(int startIndex) throws SQLException {
		
		ViewAbstractExtractor abstractExtractor = new ViewAbstractExtractor(viewType);
		ArrayList<TitleInfoVO> titleInfoVOs = abstractExtractor.getTitleInfoList(viewType);
		abstractExtractor.terminate();
		System.out.println("Total " + titleInfoVOs.size() + " entries for " + DBLPVenue.getResearchDomainShortName(viewType));
		String redirectedURL= null;
		DataManager mgr = new DataManager();
		mgr.connectmySql(DBConstants.DB_NAME);
		Connection conn = mgr.getConnection();
		String insertCitationDetail = "INSERT INTO " +
				"dblp_pub_abstracts_"+DBLPVenue.getResearchDomainShortName(viewType)+"(id, redirected_url) " +
				"VALUES(?,?)"; 
		PreparedStatement ps;
		ps = conn.prepareStatement(insertCitationDetail);
		int batchCount=0;
		for(int titleCount =startIndex ; titleCount<titleInfoVOs.size() ; titleCount++){
			System.out.println("Current Index = "+titleCount);
			String eeURL = (titleInfoVOs.get(titleCount)).getElectronicEditionURL();
			String title = (titleInfoVOs.get(titleCount)).getTitle();
			String id    = (titleInfoVOs.get(titleCount)).getId();
			//Get the redirected URL 
			redirectedURL = eeURL;
			String isRedirected= "(original)";
			if(!eeURL.endsWith("mp3")  && !eeURL.endsWith("pdf")  ){
				isRedirected= "(redirected)";
				driver.get(eeURL);
				redirectedURL = driver.getCurrentUrl();
			}
			System.out.println("ID:"+id+"  Title:"+title + "  URL" + isRedirected+ ": "+ redirectedURL);

			try{
				long val = (long)(Math.random()*CrawlerConstants.MAXSLEEPTIME);
				System.out.println("Sleeping for "+(val/1000)+" sec");
				Thread.sleep(val);
			}catch(Exception e){
				System.out.println("Interrrupted....Unable to sleep!");
			}	
			/*try {
				HttpURLConnection con = (HttpURLConnection)(new URL(eeURL).openConnection());
//				con.setInstanceFollowRedirects( false );
				con.connect();
				//int responseCode = con.getResponseCode();
				//System.out.println( responseCode );
				String location = con.getHeaderField( "Location" );
				System.out.println( location );
				if(location != null){
					redirectedURL = location.trim();
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Connection exception at index: " + titleCount + "Title: " + title + "");
				System.out.println("Reconnecting......");
				storeRedirectedURL(viewType, titleCount);
			}*/
			
			//Insert into the table dblp_pub_abstracts
			
			ps.setInt(1, Integer.parseInt(id));
			ps.setString(2, redirectedURL);
			ps.addBatch();
			batchCount++;
			if (batchCount > 500) {
				ps.executeBatch();
				batchCount=0;
				System.out.println("Inserting  : "+batchCount+" rows in View : "+DBLPVenue.getResearchDomainShortName(viewType));
			}		
		}
		int[] finalrows= ps.executeBatch();
		System.out.println("Inserting  last : "+finalrows.length+" rows in View : "+DBLPVenue.getResearchDomainShortName(viewType));
		ps.close();
		conn.close();
	}
	
}

