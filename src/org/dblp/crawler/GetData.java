package dblp.tools;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;

import com.gargoylesoftware.htmlunit.BrowserVersion;

import dblp.tools.db.DataBaseAccess;
import org.dblp.crawler.model.PaperDetails;
import org.dblp.crawler.model.PublishDetails;
@Deprecated
public class GetData {

	private static String doid = null;
	private static int refId = 0;
	private static String generatedURL = null;
	private static String citationText = null;
	private static String splitStrings[] = null;
	private static int citationCount = 0;
	private static String paperAbstract = null;
	//private static PaperDetails paperDetail = null;

	private static WebDriver driver = null;
	private static WebElement element = null;

	public static void main(String[] args) {
		try {
			System.setErr(new PrintStream("err.txt"));
		}
		catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}

		// Create a new instance of the html unit driver Notice that the remainder of the code relies on the interface, not the implementation.
		driver = new HtmlUnitDriver(BrowserVersion.FIREFOX_3);

		// connect to DB
		DataBaseAccess.getConnection();

		// get rows that satisfy condition. find URL site redirects to and store in dblp_pprdet
		/*
		List<PublishDetails> publishDetails = DataBaseAccess.getPublishDetails_all();
		// System.out.println(publishDetails.size());

		for (int i = 0; i < publishDetails.size(); i++) {
			try {
				refId = publishDetails.get(i).getId();

				if(!DataBaseAccess.doesExistIn_dblp_pprdet(refId)) {
					if(publishDetails.get(i).getEe() != null) {
						saveDetailsAll(publishDetails.get(i));
					}
					else {
						do_ee_null();
					}
					
					System.out.println("Ref. Id: " + refId + " done");
				}
				
				System.out.println("Ref. Id: " + refId + " exists");
			}
			catch (Exception e) {
				System.out.println("Exception ocurred for refId = " + refId + ". Message: " + e.getMessage());
			}
		}
		*/
		
		// try to get citation count & paper abstract if possible
		/*
		List<PaperDetails> paperDetails = DataBaseAccess.getPaperDetailsToBeDone();
		
		for (int i = 0; i < paperDetails.size(); i++) {
			try {
				refId = paperDetails.get(i).getRefId();

				if(paperDetails.get(i).getGeneratedURL() == null || paperDetails.get(i).getGeneratedURL().isEmpty())
					System.out.println("Ref. Id: " + refId + " URL is null");
				else if (paperDetails.get(i).getGeneratedURL().startsWith("http://www.computer.org"))
					do_computer_org(paperDetails.get(i));
				else if (paperDetails.get(i).getGeneratedURL().startsWith("http://portal.acm.org"))
					do_portal_acm_org(paperDetails.get(i));
				else if (paperDetails.get(i).getGeneratedURL().startsWith("http://ieeexplore.ieee.org"))
					do_ieeexplore(paperDetails.get(i));
				else if (paperDetails.get(i).getGeneratedURL().startsWith("http://www.sciencedirect.com"))
					do_sciencedirect(paperDetails.get(i));
				else if (paperDetails.get(i).getGeneratedURL().startsWith("http://www.springerlink.com"))
					do_springerlink(paperDetails.get(i));
				else
					System.out.println("Ref. Id: " + refId + " URL not supported");
				
				System.out.println("Ref. Id: " + refId + " done");
			}
			catch (Exception e) {
				System.out.println("Exception ocurred for refId = " + refId + ". Message: " + e.getMessage());
			}
		}
		*/

		try {
			driver.close();

			// close DB connection
			DataBaseAccess.closeConnection();
		}
		catch (Exception e) {
			System.out.println("Exception ocurred closing driver or connection");
		}
	}
	
	public static void saveDetailsAll(PublishDetails publishDetail) throws Exception {
		driver.get(publishDetail.getEe());
		
		generatedURL = driver.getCurrentUrl();

		DataBaseAccess.savePaperDetailsAll(new PaperDetails(refId, generatedURL, -1, "An abstract is not available."));
	}
	
	public static void do_ee_null() {
		DataBaseAccess.savePaperDetailsAll(new PaperDetails(refId, "URL could not be generated", -1, "An abstract is not available."));
	}
	
	public static void do_portal_acm_org(PaperDetails paperDetail) throws Exception {
		generatedURL = paperDetail.getGeneratedURL() + "&preflayout=flat";
		
		// load page
		driver.get(generatedURL);

		// paper title
		try {
			element = driver.findElement(By.xpath("id('divmain')/div/h1/strong"));
		}
		catch (NoSuchElementException e) {
			throw new Exception("title not found");
		}
		// System.out.println("Title: " + element.getText());

		// paper citation counts
		try {
			element = driver.findElement(By.xpath("id('fback')/div[6]"));
		}
		catch (NoSuchElementException e) {
			throw new Exception("citation not found");
		}

		citationText = element.getText();
		// System.out.println("Citation Text: " + citationText);

		if (Character.isDigit(citationText.charAt(0))) {
			splitStrings = citationText.split(" ");
			if (splitStrings.length > 1) {
				try {
					citationCount = Integer.parseInt(splitStrings[0]);
				}
				catch (NumberFormatException e) {
					throw new Exception("number not found in citation text");
				}
			}
		}
		else if (citationText == null || citationText.isEmpty() || citationText.startsWith("Citings"))
			citationCount = 0;
		else
			citationCount = 1;
		// System.out.println("Citation Count: " + citationCount);

		// paper abstract
		try {
			element = driver.findElement(By.xpath("id('fback')/div[3]/div"));
		}
		catch (NoSuchElementException e) {
			throw new Exception("abstract not found");
		}

		paperAbstract = element.getText();
		// System.out.println("Abstract: " + paperAbstract);

		//paperDetail = new PaperDetails(refId, generatedURL, citationCount, paperAbstract);
		paperDetail.setCitationCount(citationCount);
		paperDetail.setPaperAbstract(paperAbstract);

		//System.out.println(paperDetail);
		
		DataBaseAccess.savePaperDetailsCitationAbstract(paperDetail);
	}
	
	public static void do_ieeexplore(PaperDetails paperDetail) throws Exception {
		generatedURL = paperDetail.getGeneratedURL();

		// load page
		driver.get(generatedURL);

		// paper title
		try {
			element = driver.findElement(By.xpath("id('Body')/div[1]/div[2]/div[1]/h1"));
		}
		catch (NoSuchElementException e) {
			throw new Exception("title not found");
		}
		// System.out.println("Title: " + element.getText());

		// paper citation counts
		// not available publicly
		citationCount = -1;
		
		// System.out.println("Citation Count: " + citationCount);

		// paper abstract
		try {
			element = driver.findElement(By.xpath("id('Body')/div[1]/div[2]/div[3]/div[2]/p"));
		}
		catch (NoSuchElementException e) {
			throw new Exception("abstract not found");
		}

		paperAbstract = element.getText();
		// System.out.println("Abstract: " + paperAbstract);

		//paperDetail = new PaperDetails(refId, generatedURL, citationCount, paperAbstract);
		paperDetail.setCitationCount(citationCount);
		paperDetail.setPaperAbstract(paperAbstract);

		//System.out.println(paperDetail);
		
		DataBaseAccess.savePaperDetailsCitationAbstract(paperDetail);
	}
	
	public static void do_computer_org(PaperDetails paperDetail) throws Exception {
		generatedURL = paperDetail.getGeneratedURL();
		
		// load page
		driver.get(generatedURL);

		// paper title
		try {
			element = driver.findElement(By.xpath("id('abs-articleTitle')"));
		}
		catch (NoSuchElementException e) {
			throw new Exception("title not found");
		}
		// System.out.println("Title: " + element.getText());

		// paper citation counts
		citationCount = 1;
		// System.out.println("Citation Count: " + citationCount);

		// paper abstract
		try {
			element = driver.findElement(By.xpath("id('abs-abscontent')/div[2]"));
		}
		catch (NoSuchElementException e) {
			paperAbstract = "An abstract is not available.";
		}

		paperAbstract = element.getText();
		// System.out.println("Abstract: " + paperAbstract);

		//paperDetail = new PaperDetails(refId, generatedURL, citationCount, paperAbstract);
		paperDetail.setCitationCount(citationCount);
		paperDetail.setPaperAbstract(paperAbstract);

		//System.out.println(paperDetail);
		
		DataBaseAccess.savePaperDetailsCitationAbstract(paperDetail);
	}
	
	public static void do_sciencedirect(PaperDetails paperDetail) throws Exception {
		generatedURL = paperDetail.getGeneratedURL();
		
		// load page
		driver.get(generatedURL);

		// paper title
		try {
			element = driver.findElement(By.xpath("id('articleContent')/div[1]"));
		}
		catch (NoSuchElementException e) {
			throw new Exception("title not found");
		}
		// System.out.println("Title: " + element.getText());

		// paper citation counts
		// citation count not available on webpage
		citationCount = -1;
		// System.out.println("Citation Count: " + citationCount);

		// paper abstract
		try {
			element = driver.findElement(By.xpath("id('authAnchors')/div[7]/div/p"));
		}
		catch (NoSuchElementException e) {
			throw new Exception("abstract not found");
		}

		paperAbstract = element.getText();
		// System.out.println("Abstract: " + paperAbstract);

		//paperDetail = new PaperDetails(refId, generatedURL, citationCount, paperAbstract);
		paperDetail.setCitationCount(citationCount);
		paperDetail.setPaperAbstract(paperAbstract);

		//System.out.println(paperDetail);
		
		DataBaseAccess.savePaperDetailsCitationAbstract(paperDetail);
	}
	
	public static void do_springerlink(PaperDetails paperDetail) throws Exception {
		generatedURL = paperDetail.getGeneratedURL();
		
		// load page
		driver.get(generatedURL);

		// paper title
		try {
			element = driver.findElement(By.xpath("id('ContentHeading')/div[2]/div[2]/h1"));
		}
		catch (NoSuchElementException e) {
			throw new Exception("title not found");
		}
		// System.out.println("Title: " + element.getText());

		// paper citation counts
		// citation count not available
		citationCount = -1;
		// System.out.println("Citation Count: " + citationCount);

		// paper abstract
		try {
			element = driver.findElement(By.xpath("id('ContentPrimary')/div[1]/div[1]/div/div"));
		}
		catch (NoSuchElementException e) {
			throw new Exception("abstract not found");
		}

		paperAbstract = element.getText();
		// System.out.println("Abstract: " + paperAbstract);

		//paperDetail = new PaperDetails(refId, generatedURL, citationCount, paperAbstract);
		paperDetail.setCitationCount(citationCount);
		paperDetail.setPaperAbstract(paperAbstract);

		//System.out.println(paperDetail);
		
		DataBaseAccess.savePaperDetailsCitationAbstract(paperDetail);
	}
}
