package org.dblp.driver;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.dblp.dbextractor.DataManager;
import org.dblp.docprocessing.ClusterPostProcessing;
import org.dblp.docprocessing.LDAPreprocessing;
import org.dblp.docprocessing.TestSequenceFilesFromDirectory;
import org.dblp.model.BibCluster;
import org.dblp.model.BibEntry;
import org.dblp.model.ClusterMatch;

public class EntryClusterPostProcessing {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		DataManager mgr = new DataManager();
		String appBase= "out" + File.separator + "bib";
		mgr.connect("bibdb");
		// Delimiter is changed first. Hypen is not a delim here
		int count= mgr.createTextCorpusFromDB(true);
		Set<BibEntry> bibset = mgr.getEntrySet();
		ClusterPostProcessing postProc = new ClusterPostProcessing();
		postProc.setFileName(appBase + File.separator + "result" + File.separator + "dblp-kmeansclusterfile-stem.txt");
		HashMap<String, BibCluster> clusterDetails=  postProc.getClustersFromClusterDump();
		for (BibEntry anEntry : bibset) {
			String[] doc = anEntry.getBibEntryAsArrayOfString();
			ClusterMatch match = postProc.getClosestCluster(anEntry, clusterDetails);
			System.out.println(match);
		}
	}

}
