package org.dblp.crawler.model;

public class CitationLinkVO {
	private int publicationId;
	private String publicationTitle;
	private int citationCount;
	private String citationLink;

	public CitationLinkVO(int id, String publicationTitle,
			int citationCount, String citationLink) {
		super();
		this.publicationId = id;
		this.publicationTitle = publicationTitle;
		this.citationCount = citationCount;
		this.citationLink = citationLink;
	}

	public int getPublicationId() {
		return publicationId;
	}

	public void setPublicationId(int id) {
		this.publicationId = id;
	}

	public String getPublicationTitle() {
		return publicationTitle;
	}

	public void setPublicationTitle(String publicationTitle) {
		this.publicationTitle = publicationTitle;
	}

	public int getCitationCount() {
		return citationCount;
	}

	public void setCitationCount(int citationCount) {
		this.citationCount = citationCount;
	}

	public String getCitationLink() {
		return citationLink;
	}

	public void setCitationLink(String citationLink) {
		this.citationLink = citationLink;
	}
}
