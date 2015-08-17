package dblp.tools.model;
@Deprecated
public class PaperDetails {
	private int id;
	private int refId;
	private String generatedURL;
	private int citationCount;
	private String paperAbstract;
	
	public PaperDetails(int refId, String generatedURL, int citationCount, String paperAbstract) {
		super();
		this.refId = refId;
		this.generatedURL = generatedURL;
		this.citationCount = citationCount;
		this.paperAbstract = paperAbstract;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getRefId() {
		return refId;
	}
	public void setRefId(int refId) {
		this.refId = refId;
	}
	public String getGeneratedURL() {
		return generatedURL;
	}
	public void setGeneratedURL(String generatedURL) {
		this.generatedURL = generatedURL;
	}
	public int getCitationCount() {
		return citationCount;
	}
	public void setCitationCount(int citationCount) {
		this.citationCount = citationCount;
	}
	public String getPaperAbstract() {
		return paperAbstract;
	}
	public String getSafePaperAbstract() {
		return paperAbstract.replaceAll("'", "&apos;");
	}
	public String getOrgPaperAbstract() {
		return paperAbstract.replaceAll("&apos;", "'");
	}
	public void setPaperAbstract(String paperAbstract) {
		this.paperAbstract = paperAbstract;
	}
	
	public String toString() {
		return refId + " " + generatedURL + " " + citationCount + " " + paperAbstract;
	}
}
