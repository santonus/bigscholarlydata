package org.dblp.crawler.model;

public class TitleInfoVO{

	private String title;
	private String id;
	private String electronicEditionURL; //maps to the ee field in the dblp_pub_new field
	private String pubAbstract;
	private String redirectedURL;
	private String viewType;
	private String year;
	
	
	public TitleInfoVO(String title, String id, String year,String electronicEditionURL,
			String pubAbstract, String redirectedURL, String viewType) {
		super();
		this.title = title;
		this.id = id;
		this.electronicEditionURL = electronicEditionURL;
		this.pubAbstract = pubAbstract;
		this.redirectedURL = redirectedURL;
		this.viewType = viewType;
		this.setYear(year);
	}
	
	public TitleInfoVO(String title, String id, String electronicEditionURL) {
		super();
		this.title = title;
		this.id = id;
		this.setElectronicEditionURL(electronicEditionURL);
	}

	public TitleInfoVO(String title,String id) {
		this.title=title;
		this.id=id;
	}
	

	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

	public void setElectronicEditionURL(String electronicEditionURL) {
		this.electronicEditionURL = electronicEditionURL;
	}

	public String getElectronicEditionURL() {
		return electronicEditionURL;
	}

	public void setPubAbstract(String pubAbstract) {
		this.pubAbstract = pubAbstract;
	}

	public String getPubAbstract() {
		return pubAbstract;
	}

	public void setRedirectedURL(String redirectedURL) {
		this.redirectedURL = redirectedURL;
	}

	public String getRedirectedURL() {
		return redirectedURL;
	}

	public void setViewType(String viewType) {
		this.viewType = viewType;
	}

	public String getViewType() {
		return viewType;
	}

	@Override
	public String toString() {
		return "TitleInfoVO [electronicEditionURL=" + electronicEditionURL
				+ ", id=" + id + ", pubAbstract=" + pubAbstract
				+ ", redirectedURL=" + redirectedURL + ", title=" + title
				+ ", viewType=" + viewType + "]";
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getYear() {
		return year;
	}
	
	
	
}
