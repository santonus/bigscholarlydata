package dblp.tools.model;
@Deprecated
public class PublishDetails {
	private int id;
	private String doi;
	private String ee;
	
	public PublishDetails(int id, String doi, String ee) {
		super();
		this.id = id;
		this.doi = doi;
		this.ee = ee;
	}

	public String getExtractedDOID() {
		if(doi != null) {
			String parts[] = doi.split("/");
			
			if(parts.length > 1)
				return parts[1];
		}
		
		return null;
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public String getEe() {
		return ee;
	}

	public void setEe(String ee) {
		this.ee = ee;
	}
}
