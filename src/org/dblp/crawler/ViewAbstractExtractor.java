package org.dblp.crawler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;

import org.dblp.dbextractor.DBConstants;
import org.dblp.dbextractor.DBLPVenue;
import org.dblp.dbextractor.DataManager;
import org.dblp.crawler.model.TitleInfoVO;


public class ViewAbstractExtractor {

	private static WebDriver driver = null;
	private static WebElement element = null;
	int viewType;
	private Connection conn; 

	public ViewAbstractExtractor(int vt){
		driver = new HtmlUnitDriver();
		viewType = vt;
		if(CrawlerConstants.HTTP_PROXY != null && CrawlerConstants.HTTP_PROXY.length()>0){
			((HtmlUnitDriver) driver).setProxy(CrawlerConstants.HTTP_PROXY, Integer.parseInt(CrawlerConstants.HTTP_PORT));
			System.setProperty("http.proxyHost", CrawlerConstants.HTTP_PROXY);
			System.setProperty("http.proxyPort", CrawlerConstants.HTTP_PORT);
			System.setProperty("http.proxyUser", CrawlerConstants.HTTP_USER);
			System.setProperty("http.proxyPassword", CrawlerConstants.HTTP_PWD);
			System.setProperty("http.proxyAuth", CrawlerConstants.HTTP_AUTH);
		}
		try {
			System.setOut(new PrintStream(new FileOutputStream(CrawlerConstants.OUTPUTDIRPATH+File.separator+
					CrawlerConstants.CRAWLRESDIR+File.separator+ DBLPVenue.getResearchDomainShortName(vt) + 
					"_Abstract_Results_Console.log", true),true));
			Timestamp now = new Timestamp(System.currentTimeMillis());
			System.out.println("------------------------------------------------------------------------------------------------------");
			System.out.println("Current Time........."+now.toString());
			System.setErr(new PrintStream(new FileOutputStream(CrawlerConstants.OUTPUTDIRPATH+File.separator+
					CrawlerConstants.CRAWLRESDIR+File.separator+ DBLPVenue.getResearchDomainShortName(vt) + 
					"_Abstract_Results_Error.log",true),true));
			now = new Timestamp(System.currentTimeMillis());
			System.err.println("--------------------------------------------------------------------------------------------------------");
			System.err.println("Current Time........."+now.toString());
			DataManager mgr= new DataManager();
			mgr.connectmySql(DBConstants.DB_NAME);
			conn = mgr.getConnection();
		}
		catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
	}
	
/*	public static void main(String[] args) throws Exception {
		
		ViewAbstractExtractor abstractExtractor = new ViewAbstractExtractor(DBLPVenue.AI);
		
		abstractExtractor.extractAbstractsFromView(0);
		//abstractExtractor.extractAbstractsFromView(DBLPVenue.DB,0);
		
	} */
	public void terminate() throws SQLException {
		conn.close();
	}

	public void extractAbstractsFromView(int startIndex) {
		ArrayList<String> whereClauseList = new ArrayList<String>();
		//whereClauseList.add(" view_type = '"+DBLPVenue.getResearchDomainShortName(viewType)+"' \n");
		whereClauseList.add(" abstract_content is null \n");
		whereClauseList.add("  redirected_url not like '%pdf' ");
		whereClauseList.add("  redirected_url not like '%mp3' ");
		//whereClauseList.add(" redirected_url like 'http://www.computer.org%' \n");
		ArrayList<TitleInfoVO> titleInfoVOs = selectFromDBLPPubAbstracts(whereClauseList,viewType);
		for (int titleCount = startIndex; titleCount < titleInfoVOs.size(); titleCount++) {
			TitleInfoVO infoVO = titleInfoVOs.get(titleCount);
			
			String id = infoVO.getId();
			String redirectedURL = infoVO.getRedirectedURL();
			
			try{
			
				System.out.println("Index........"+titleCount+"  Current Redirected URL..."+redirectedURL);
				System.out.println("Ref.Id: ........"+id);
				
			
			if(redirectedURL == null || redirectedURL.isEmpty())
				System.out.println("Ref. Id: " + id + " URL is null");
			else if (redirectedURL.endsWith("mp3")  || redirectedURL.endsWith("pdf"))
				System.out.println("Cannot parse a URL that ends with mp3 or pdf");
			else if (redirectedURL.startsWith("http://www.computer.org"))
				parseAndSave_ComputerOrg(infoVO);
			else if (redirectedURL.startsWith("http://dl.acm.org"))
				parseAndSave_PortalAcmOrg(infoVO);
			else if (redirectedURL.startsWith("http://ieeexplore.ieee.org"))
				parseAndSave_IeeeExplore(infoVO);
			else if (redirectedURL.startsWith("http://www.sciencedirect.com"))
				parseAndSave_ScienceDirect(infoVO);
			else if (redirectedURL.startsWith("http://www.springerlink.com"))
				parseAndSave_SpringerLink(infoVO);
			else if (redirectedURL.startsWith("http://www.worldscinet.com"))
				parseAndSave_WorldSciNet(infoVO);
			else if (redirectedURL.startsWith("http://www.usenix.org"))
				parseAndSave_Usenix(infoVO);
			else if (redirectedURL.startsWith("http://www.tandfonline.com"))
				parseAndSave_T_And_F_Online(infoVO);
			else
				System.out.println("Ref. Id: " + id + " URL not supported");
			}catch(Exception e){
				e.printStackTrace();
				System.out.println("Exception ocurred for refId = " + id + ". Message: " + e.getMessage());
			}

			System.out.println();
			System.out.flush();
		}
		

	}

	private void parseAndSave_T_And_F_Online(TitleInfoVO infoVO) throws Exception {
		infoVO.setPubAbstract(null);
		String url = infoVO.getRedirectedURL();
		String abs = null;
		Document doc;
		try {
			doc = Jsoup.connect(url).timeout(0).get();
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("Jsoup Failed To Connect to "+url);
		}
		//System.out.println(doc);
		if(doc != null){
			Elements elements = doc.select("div[class=abstract module borderedmodule-last]").select("p[class]");;
			if(elements.size() == 1)
				for (Element element : elements) {
					abs = element.text().trim();
				}
		}
		infoVO.setPubAbstract(abs);
		saveAbstractContentInDBLP_Pub_Abstract(infoVO);
	}

	private void parseAndSave_Usenix(TitleInfoVO infoVO) throws Exception{
		
		infoVO.setPubAbstract(null);
		String url = infoVO.getRedirectedURL();
		String abs = null;
		
		if((abs = crawlUsenix_Simple_Page(url))==null)
			abs = crawlUsenix_Complex_Page(url);
		
		infoVO.setPubAbstract(abs.trim());
		
		saveAbstractContentInDBLP_Pub_Abstract(infoVO);
		
	}
	
	private String crawlUsenix_Simple_Page(String url) throws Exception{
		System.out.println("--->crawlUsenix_Simple_Page..............");
		driver.get(url);
		
		try {
			org.openqa.selenium.WebElement element = driver.findElement(By.xpath("//body"));
			String abs = "";
			//System.out.println("In testUseINX ==>"+element.getText());
			String[] elements =  element.getText().split("\n") ;
			int length = element.getText().split("\n").length;
			boolean flag = false;
			for (int i = 0; i < length; i++) {
				//System.out.println("testUseINX() : "+i+"==>"+elements[i].trim());
				if("Abstract".equals(elements[i].trim())){
					//return elements[i+1].trim();
					//System.out.println("######### Abstratc FOund ##### ");
					flag = true;
				}
				if(elements[i].trim().startsWith("Download the full text")){
					//System.out.println("Ending text found");
					flag = false;
					break;
				}													
				if(flag){
					abs+=elements[i].trim();
				}
			}
			if(!flag)
			return "".equals(abs)?null:abs.replaceFirst("Abstract", "");
		}
		catch (NoSuchElementException e) {
			throw new Exception("Abstract Not Found");
		}
		return null;
	}
	
	private String crawlUsenix_Complex_Page(String url) throws Exception{
		System.out.println("--->crawlUsenix_Complex_Page..............");
		Document doc;
		try {
			doc = Jsoup.connect(url).timeout(0).get();
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("Jsoup Failed To Connect to "+url);
		}
		if(doc != null){
			String abs = "";
			if(doc.text().contains("View the full text")){
				abs = doc.text().replaceAll("\n","").replaceAll(".*(Abstract*)(.*?)Abstract", "").replaceAll(" View the full text.*$", "");
			}
			if(abs.contains("Abstract")){
				abs = abs.substring(abs.lastIndexOf("Abstract")).replaceAll("^Abstract","");
			}
			return "".equals(abs)?null:abs.trim();  
			
		}
		return null;

	}		
	
	
	private void parseAndSave_WorldSciNet(TitleInfoVO infoVO) throws Exception {
		infoVO.setPubAbstract(null);
		Document doc;
		try {
			doc = Jsoup.connect(infoVO.getRedirectedURL()).timeout(0).get();
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("Jsoup Failed To Connect to "+infoVO.getRedirectedURL());
		}
		
		if(doc != null){
			Elements links = doc.select("td[align=right][width=10%][class=text]");
			for (org.jsoup.nodes.Element element : links) {
				if("Abstract:".equals(element.text())){
					infoVO.setPubAbstract(element.parent().text().replaceFirst("Abstract:", "").trim());
					saveAbstractContentInDBLP_Pub_Abstract(infoVO);
				}
			}
		}

	}

	private void parseAndSave_SpringerLink(TitleInfoVO titleInfoVO) throws Exception {
		String generatedURL = titleInfoVO.getRedirectedURL();
		//String generatedURL = "http://www.springerlink.com/content/4klne8anq2p85gq1/";
		// load page
		driver.get(generatedURL);
		// paper abstract
		try {
			element = driver.findElement(By.xpath("id('ContentPrimary')/div[1]/div[1]/div"));
		}
		catch (NoSuchElementException e) {
			titleInfoVO.setPubAbstract("Abstract Not Found");
			saveAbstractContentInDBLP_Pub_Abstract(titleInfoVO);
			throw new Exception("Abstract Not Found");
		}

		saveAbstractContentInDBLP_Pub_Abstract(titleInfoVO);		
		
		
	}

	private void parseAndSave_ScienceDirect(TitleInfoVO infoVO) throws Exception {
		String generatedURL = infoVO.getRedirectedURL();
		//String generatedURL = "http://www.sciencedirect.com/science/article/pii/S1477842404000132";
		// load page
		driver.get(generatedURL);
		// paper abstract
		try {
			element = driver.findElement(By.xpath("//div[@class = \"svAbstract\"]"));
		}
		catch (NoSuchElementException e) {
			infoVO.setPubAbstract("Abstract Not Found");
			saveAbstractContentInDBLP_Pub_Abstract(infoVO);
			throw new Exception("Abstract Not Found");
		}

		saveAbstractContentInDBLP_Pub_Abstract(infoVO);		
		
	}

	private void parseAndSave_IeeeExplore(TitleInfoVO infoVO) throws Exception {
		String generatedURL = infoVO.getRedirectedURL();
		//String generatedURL = "http://ieeexplore.ieee.org/xpl/freeabs_all.jsp?arnumber=4741166&reason=concurrency";
		// load page
		driver.get(generatedURL);
		// paper abstract
		try {
			element = driver.findElement(By.xpath("id('Body')/div[1]/div[2]/div[3]/div[2]/p"));
		}
		catch (NoSuchElementException e) {
			infoVO.setPubAbstract("Abstract Not Found");
			saveAbstractContentInDBLP_Pub_Abstract(infoVO);

			throw new Exception("Abstract Not Found");
		}
		
		saveAbstractContentInDBLP_Pub_Abstract(infoVO);
		
	}

	private void parseAndSave_PortalAcmOrg(TitleInfoVO infoVO) throws Exception {
		String generatedURL = infoVO.getRedirectedURL() + "&preflayout=flat";
		//String generatedURL = "http://dl.acm.org/citation.cfm?doid=291069.291026" + "&preflayout=flat";
		// load page
		driver.get(generatedURL);
		
		// paper abstract
		try {
			element = driver.findElement(By.xpath("id('fback')/div[3]/div"));
		}
		catch (NoSuchElementException e) {
			infoVO.setPubAbstract("Abstract Not Found");
			saveAbstractContentInDBLP_Pub_Abstract(infoVO);
			throw new Exception("Abstract Not Found");
		}
		saveAbstractContentInDBLP_Pub_Abstract(infoVO);
		
	}

	private void parseAndSave_ComputerOrg(TitleInfoVO infoVO) throws Exception {
		String generatedURL = infoVO.getRedirectedURL();
		//String generatedURL = "http://www.computer.org/portal/web/csdl/abs/proceedings/hotos/1999/0237/00/02370052abs.htm";
		// load page
		driver.get(generatedURL);
		
		// paper abstract
		try {
			element = driver.findElement(By.xpath("id('abs-abscontent')/div[2]"));
		}
		catch (NoSuchElementException e) {
			infoVO.setPubAbstract("Abstract Not Found");
			saveAbstractContentInDBLP_Pub_Abstract(infoVO);
			throw new Exception("Abstract Not Found");
		}
	
		saveAbstractContentInDBLP_Pub_Abstract(infoVO);
		
	}


	private void saveAbstractContentInDBLP_Pub_Abstract(TitleInfoVO infoVO) {
		
		if(element != null){
			String paperAbstract = element.getText();
			infoVO.setPubAbstract(paperAbstract);
			System.out.println("Abstract: " + paperAbstract);
			updateAbstractContent(infoVO);
		}else if(infoVO!=null && infoVO.getPubAbstract()!=null){
			System.out.println("Abstract: " + infoVO.getPubAbstract());
			updateAbstractContent(infoVO);
		}
		
		try {
			Thread.sleep((long) (312345 * Math.random()));
		} catch (InterruptedException e1) {
			System.out.println("Failed to sleep!");
			e1.printStackTrace();
		}

		
	}
	
	private void updateAbstractContent(TitleInfoVO titleInfoVO){
/*		DataManager mgr = new DataManager();
		mgr.connectmySql(DBConstants.DB_NAME);
		Connection conn = mgr.getConnection();
*/		
		String updateAbstractDetail = "UPDATE  " +
		"dblp_pub_abstracts_"+titleInfoVO.getViewType()+" set  abstract_content = ? where id = ?" ;
		
		System.out.println("Saving abstract in database......................");
		
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(updateAbstractDetail);
			ps.setString(1, titleInfoVO.getPubAbstract());
			ps.setInt(2, Integer.parseInt(titleInfoVO.getId()));
			
			ps.executeUpdate();
			
			ps.close();
			// conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Could not save abstract for id = "+titleInfoVO.getId());
		}
		
	}

	public ArrayList<TitleInfoVO> getTitleInfoList(int viewCode) {
		String  viewName =  DBLPVenue.getResearchDomainViewName(viewCode);
		 
		String selectTitlesWithExternalURL = "SELECT id,title,ee from "+viewName+" WHERE ee IS NOT NULL ORDER BY id";
		ArrayList<TitleInfoVO> titleInfoList = new ArrayList<TitleInfoVO>();
		/*
		DataManager mgr = new DataManager();
		mgr.connectmySql(DBConstants.DB_NAME);
		Connection conn = mgr.getConnection(); */

		Statement st;
		ResultSet rs;
		try {
			st = conn.createStatement();
			rs = st.executeQuery(selectTitlesWithExternalURL);
			while (rs.next()) {
				String id = rs.getString("id");
				String title = rs.getString("title");
				String ee = rs.getString("ee");
				titleInfoList.add(new TitleInfoVO(title,id,ee));
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return titleInfoList;
		
	}
	
	public ArrayList<TitleInfoVO> selectFromDBLPPubAbstracts(ArrayList<String> whereClauseList,int viewType){		
		ArrayList<TitleInfoVO> titleInfoList = new ArrayList<TitleInfoVO>();
		String selectFromDBLPAbstracts = "SELECT * FROM dblp_pub_abstracts_"+DBLPVenue.getResearchDomainShortName(viewType) ;
/*		DataManager mgr = new DataManager();
		mgr.connectmySql(DBConstants.DB_NAME);
		Connection conn = mgr.getConnection(); */

		Statement st;
		ResultSet rs;
		if(!whereClauseList.isEmpty()){	
			selectFromDBLPAbstracts += " WHERE ";
			for (Iterator iterator = whereClauseList.iterator(); iterator.hasNext();) {
				String whereClause = (String) iterator.next();
				selectFromDBLPAbstracts += whereClause ; 
				selectFromDBLPAbstracts += " AND " ;
			}
			//remove the last AND
			if(selectFromDBLPAbstracts.endsWith(" AND ")){
				selectFromDBLPAbstracts = selectFromDBLPAbstracts.substring(0, selectFromDBLPAbstracts.length() - " AND ".length()); 
			}
		}
		
		//finally  query by order by id
		//selectFromDBLPAbstracts = selectFromDBLPAbstracts +" order by id ";
		
		System.out.println("Final Select Query...................." + selectFromDBLPAbstracts);
	
		try {
			st = conn.createStatement();
			rs = st.executeQuery(selectFromDBLPAbstracts);
			while (rs.next()) {
				String id = rs.getString("id");
				String redirectedURL = rs.getString("redirected_url");
				String strViewType = DBLPVenue.getResearchDomainShortName(viewType);
				String abstractContent = rs.getString("abstract_content");
				TitleInfoVO titleInfoVO = new TitleInfoVO(null,id,null,null,abstractContent,redirectedURL,strViewType);
				titleInfoList.add(titleInfoVO);
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return titleInfoList;
	}
}
