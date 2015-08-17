package org.dblp.model;

public class ClusterMatch {
	private BibCluster _cluster;
	private BibEntry _bib;
	private double _similarity;
	public final BibCluster getCluster() {
		return _cluster;
	}
	public final void setCluster(BibCluster cluster) {
		this._cluster = cluster;
	}
	public final BibEntry getBib() {
		return _bib;
	}
	public final void setBib(BibEntry bib) {
		this._bib = bib;
	}
	public final double getSimilarity() {
		return _similarity;
	}
	public final void setSimilarity(double similarity) {
		this._similarity = similarity;
	}
	@Override
	public final String toString() {
		StringBuffer strbuf = new StringBuffer();
		strbuf.append(_bib.getTitleAsBOW());
		strbuf.append("==>");
		strbuf.append(_cluster.getClusterName());
		strbuf.append("==>");
		strbuf.append(_similarity);
		return strbuf.toString();
		
	}
}
