package org.dblp.driver;

import java.io.File;

import org.dblp.crawler.CrawlerConstants;
import org.dblp.crawler.PublicationCitationExtractor;
import org.dblp.crawler.RedirectURLExtractor;
import org.dblp.crawler.ViewAbstractExtractor;
import org.dblp.dbextractor.DBLPVenue;

public class WebCrawler {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		init();
		System.out.println("Starting Abstract pulling...");
		int startIndex = 0;
//		redirectURLS(startIndex);
		pullAbstracts(startIndex);
//		pullCitations(startIndex);
	}
	public static void init() {
		File file = new File(CrawlerConstants.OUTPUTDIRPATH + File.separator + CrawlerConstants.CRAWLRESDIR);
		file.mkdirs();
	}
	private static void redirectURLS(int startIndex) throws Exception {
		RedirectURLExtractor redirectURLExtractor = new RedirectURLExtractor(DBLPVenue.AI);
		redirectURLExtractor.storeRedirectedURL(startIndex);
	}
	private static void pullAbstracts(int startIndex) throws Exception {
		ViewAbstractExtractor abstractExtractor = new ViewAbstractExtractor(DBLPVenue.AI);
		try {
			abstractExtractor.extractAbstractsFromView(startIndex);
		} finally {
			abstractExtractor.terminate();
		}
	}
	private static void pullCitations(int startIndex) throws Exception {
		PublicationCitationExtractor citationExtractor = new PublicationCitationExtractor(DBLPVenue.AI);
		try {
			citationExtractor.getPublicationInfoFromView();
			citationExtractor.parseAndStoreSearchResultsForTitleQuery(startIndex);
			System.out.println("Storing Citation links into DB... ");
			citationExtractor.parseAndStoreLinksForCitations();
		} finally {
			citationExtractor.close();
		}
	}

}
