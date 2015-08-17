package org.dblp.docprocessing;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dblp.model.BibCluster;
import org.dblp.model.BibEntry;
import org.dblp.model.ClusterMatch;

public class ClusterPostProcessing {
	private final String topWordPattern = "(\\S+)\\s*=>([\\d\\.]*)";
	private final String wordPattern = "^(\\S+)";
	private final String clusterPattern = "\\:(\\S+-\\d+)";
	private final String topicPattern = "Topic\\s*\\d+";
	
	private String _fileName;
	
	public void setFileName(String s) {
		_fileName= s;
	}
	
	public HashMap<String, BibCluster> getClustersFromClusterDump() throws IOException {
		return getClustersFromFile(clusterPattern,topWordPattern);
	}
	public HashMap<String, BibCluster> getClustersFromTopicDump() throws IOException {
		return getClustersFromFile(topicPattern,wordPattern);
	}
	
	private HashMap<String, BibCluster> getClustersFromFile(String clusterPat, String wordPat) throws IOException {
		HashMap<String, BibCluster> clusterSet= new HashMap<String,BibCluster>();
		BufferedReader reader= new BufferedReader(new InputStreamReader(new FileInputStream(_fileName)));
		String nextLine= null;
		Pattern wp = Pattern.compile(wordPat);
		Pattern cp = Pattern.compile(clusterPat);
		HashSet<String> currTopWordSet = null;
		String currClusterName= null;
		int currClusterId=0;
		while ((nextLine=reader.readLine())!=null) {
			Matcher m1 = cp.matcher(nextLine);
			if (m1.find()) {
				currClusterName = m1.group(1); 
				if (clusterSet.containsKey(currClusterName) ==false) {
					BibCluster cluster= new BibCluster();
					currClusterId++;
					cluster.setClusterId(currClusterId);
					cluster.setClusterName(currClusterName);
					cluster.setTopWords(new HashSet<String>());
					clusterSet.put(currClusterName,cluster);
				}
				continue;
			}
			Matcher m2 = wp.matcher(nextLine);
			if (m2.find()) {
				currTopWordSet= clusterSet.get(currClusterName).getTopWords();
				String topword = m2.group(1);
				currTopWordSet.add(topword);
				continue;
			}
		}
		reader.close();
		return clusterSet;
	}
	
	private double similarity(String[] doc, HashSet<String> cluster ) {
		if (doc == null || doc.length==0 || cluster== null || cluster.size()==0)
			return 0.0;
		int docSize= doc.length;
		int match=0;
		double sim1=0.0;
		for (int i=0; i < docSize; i++) {
			if (cluster.contains(doc[i])==true)
				match++;
		}
		sim1= (double)match/(double)docSize;
		match =0;
		for (String word : cluster) {
			for (int i=0; i < docSize; i++) {
				if (word.equals(doc[i])==true) {
					match++; break;
				}
			}
		}
		double sim = (sim1 + (double)match/(double)cluster.size())/2.0;
		return sim;
	}
	
	public ClusterMatch getClosestCluster(BibEntry anEntry, HashMap<String, BibCluster> clusterSet) {
		double maxSim = 0.0;
		BibCluster maxCluster = null;
		String[] doc = anEntry.getBibEntryAsArrayOfString();
		ClusterMatch match = null;
		for (String clusterName : clusterSet.keySet()) {
			BibCluster cluster = clusterSet.get(clusterName);
			HashSet<String> topwords = cluster.getTopWords();
			double thisMatch = similarity(doc, topwords);
			if (thisMatch > maxSim) {
				maxSim= thisMatch;
				maxCluster = cluster;
			}
		}
		if (maxSim > 0.0) {
			match = new ClusterMatch();
			match.setBib(anEntry);
			match.setCluster(maxCluster);
			match.setSimilarity(maxSim);
		}
		return match;
	}
	
}
