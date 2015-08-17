package org.dblp.model;

import java.util.HashSet;

public class BibCluster {
	private int _clusterId;
	private String _clusterName;
	private HashSet<String> _topWords;

	public final int getClusterId() {
		return _clusterId;
	}
	public final void setClusterId(int clusterId) {
		this._clusterId = clusterId;
	}
	public final String getClusterName() {
		return _clusterName;
	}
	public final void setClusterName(String clusterName) {
		this._clusterName = clusterName;
	}
	public final HashSet<String> getTopWords() {
		return _topWords;
	}
	public final void setTopWords(HashSet<String> topWords) {
		this._topWords = topWords;
	}
	@Override
	public boolean equals(Object o) {
		BibCluster anotherOne = (BibCluster)o;
		boolean eq= _clusterName.equals(anotherOne.getClusterName());
		return eq;
	}
	
}
