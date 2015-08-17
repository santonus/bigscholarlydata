package org.dblp.crawler;

public interface CrawlerConstants {
	String OUTPUTDIRPATH= ".\\out";
	String CRAWLRESDIR= "Crawled_Search_Results";
//	String HTTP_PROXY = "192.168.3.254";
	String HTTP_PROXY = "10.66.184.116";
	String HTTP_USER = null;
	String HTTP_PWD = null;
	String HTTP_AUTH = "basic";
	String HTTP_PORT = "80";
	int MAXSLEEPTIME = 1000*5;
}
