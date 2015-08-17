package org.dblp.driver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import ir.utilities.Weight;
import ir.vsr.DocumentIterator;
import ir.vsr.DocumentReference;
import ir.vsr.FileDocument;
import ir.vsr.HashMapVector;
import ir.vsr.InvertedIndex;
import ir.vsr.TextFileDocument;
import ir.vsr.TextStringDocument;
import ir.vsr.TokenInfo;
import ir.vsr.TokenOccurrence;

public class InvertedNdxDriver {
	class KeyValuePair implements Comparable<KeyValuePair>{
		String token;
		double match;
		KeyValuePair(String t, double m) {
			token = t;
			match = m;
		}
		@Override
		public int compareTo(KeyValuePair o) {
			if (match > o.match)
				return 1;
			else if (match < o.match)
				return -1;
			else
				return 0;
		} 
		@Override
		public boolean equals(Object o) {
			KeyValuePair kvp = (KeyValuePair)o;
			return (token.equalsIgnoreCase(kvp.token));
		}
		@Override
		public String toString() {
			return ("(" + token + "," + match + ")");
		}
	}
	class DocInfo {
		ArrayList<KeyValuePair> orderedTerms;
		double maxFreq;
		DocInfo() {
			orderedTerms= new ArrayList<KeyValuePair>();
		}
	}
	class KVPairRevComp implements Comparator<KeyValuePair> {
		@Override
		public int compare(KeyValuePair o2, KeyValuePair o1) {
			if (o1.match > o2.match)
				return 1;
			else if (o1.match < o2.match)
				return -1;
			else
				return 0;
		}
	}
	private HashMap<DocumentReference, DocInfo> forwardNdx;
	private InvertedIndex invNdx;
	/**
	 * @param args
	 */
	

	
	public void createAllIndex(String dirName, boolean stem) throws IOException {
		short docType = DocumentIterator.TYPE_DOMTEXT;
		boolean feedback = false;
		TextFileDocument.tokenizerDelim = " \t\n\r\f\'\"\\1234567890!@#$%^&*()_+={}|[]:;<,>.?/`~"; 
		invNdx = new InvertedIndex(new File(dirName), docType, stem, feedback);
		forwardNdx = new HashMap<DocumentReference, DocInfo>();
		int count=0;
		KVPairRevComp revcomp = new KVPairRevComp();
		for (DocumentReference aDocRef : invNdx.docRefs) {
			HashMapVector terms = aDocRef.getDocument(docType, stem).hashMapVector();
			DocInfo aDocInfo = new DocInfo(); 
			aDocInfo.maxFreq = terms.maxWeight();
			for (Map.Entry<String,Weight> entry : terms.entrySet()) {
				String aTerm = entry.getKey();
				TokenInfo thisTokenInfo = invNdx.tokenHash.get(aTerm);
				if (thisTokenInfo != null && thisTokenInfo.idf > 0.0) {
					double weight = thisTokenInfo.idf * terms.getWeight(aTerm)/aDocInfo.maxFreq;
					aDocInfo.orderedTerms.add(new KeyValuePair(aTerm,weight));
				}
			}
			forwardNdx.put(aDocRef, aDocInfo);
//			count += aDocInfo.orderedTerms.size();
//			System.out.println(aDocRef.file.getName() + "::" + aDocInfo.orderedTerms);
			Collections.sort(aDocInfo.orderedTerms, revcomp);
//			System.out.println(aDocInfo.orderedTerms);
		}
//		System.out.println(count);
	}

/*	public void createAllIndex(String dirName, boolean stem) throws IOException {
		short docType = DocumentIterator.TYPE_TEXT;
		boolean feedback = false;
		TextFileDocument.tokenizerDelim = " \t\n\r\f\'\"\\1234567890!@#$%^&*()_+={}|[]:;<,>.?/`~"; 
		invNdx = new InvertedIndex(new File(dirName), docType, stem, feedback);
		forwardNdx = new HashMap<DocumentReference, DocInfo>();
		//DEBUG
		DocumentReference modRef = null;
		for (Map.Entry<String,TokenInfo> entry : invNdx.tokenHash.entrySet()) {
		    String token = entry.getKey();
		    for(TokenOccurrence occ : entry.getValue().occList) {
		    	DocInfo aDocInfo = forwardNdx.get(occ.docRef);
		    	if (aDocInfo==null) {
		    		aDocInfo = new DocInfo(); 
		    		forwardNdx.put(occ.docRef, aDocInfo);
		    	}
		    	double weight = entry.getValue().idf * occ.count;
				aDocInfo.orderedTerms.add(new KeyValuePair(token,weight));
			if (token.equalsIgnoreCase("modularization")) {
					modRef=occ.docRef;
					System.out.println(token + " " + weight + " " + forwardNdx.get(occ.docRef).orderedTerms);
				}
	    } 
		} 
		int c1=0;
		System.out.println("InvNdx size" + invNdx.tokenHash.size());
		for (DocumentReference ar : forwardNdx.keySet())
			System.out.println(ar.file.getName() + "=" + (c1+=forwardNdx.get(ar).orderedTerms.size()));
	} */

	private boolean shouldConsider(String keywd) {
		if (keywd.length() < 3 || keywd.endsWith("-") || keywd.startsWith("-") )
			return false;
		else return true;
	}
	public HashMap<String, HashSet<String>> dumpTopKeywords(double percentage, boolean shouldPrint) {
		HashMap<String, HashSet<String>> dumper = new HashMap<String, HashSet<String>>();
		for (DocumentReference aDocRef : forwardNdx.keySet()) {
			DocInfo aDocInfo = forwardNdx.get(aDocRef);
			if (shouldPrint)
				System.out.println(aDocRef.file.getName());
		    double count = aDocInfo.orderedTerms.size() * percentage;
		    HashSet<String> topkeywords= new HashSet<String>();
		    dumper.put(aDocRef.file.getName(), topkeywords);
		    int i=0;
		    for (KeyValuePair val : aDocInfo.orderedTerms) {
		    	if (i > count)
		    		break;
		    	if (shouldConsider(val.token)== false)
		    		continue;
				topkeywords.add(val.token);
				if (shouldPrint)
					System.out.print( "   " + val);
				i++;
		    }
		    if (shouldPrint)
				System.out.print("\n\n");
		}
		return dumper;
	}
	public static void main(String[] args) throws Exception{
		if (args.length < 2) {
			System.out.println("Usage: java -jar dblp.jar [Options] top%-(between 0-1) timestepInputDir authorListInputDir");
			System.exit(1);
		}
		String authorListInputDir = args[args.length - 1];	
		String timestepInputDir = args[args.length - 2];
		double percentage = Double.valueOf(args[args.length - 3]);
		short docType = DocumentIterator.TYPE_TEXT;
		boolean stem = false, feedback = false;
		for(int i = 0; i < args.length - 3; i++) {	
		   String flag = args[i];
		   if (flag.equals("-html"))
			// Create HTMLFileDocuments to filter HTML tags
			docType = DocumentIterator.TYPE_HTML;
		   else if (flag.equals("-stem"))
		       // Stem tokens with Porter stemmer
		       stem = true;
		   else if (flag.equals("-feedback"))
		       // Use relevance feedback
		       feedback = true;
		   else {
		       System.out.println("\nUnknown flag: " + flag);
		       System.exit(1);
		   }
		}
	
		InvertedNdxDriver driver = new InvertedNdxDriver();
		driver.createAllIndex(timestepInputDir, stem);
		
		HashMap<String, HashSet<String>> topTopics= driver.dumpTopKeywords(percentage, false);
		TreeSet<String> consolidatedKeywds = new TreeSet<String>();
		System.out.println("Original Unique Keywords in TitleList: " + driver.invNdx.tokenHash.size());
		System.out.print("Consolidating top " + percentage * 100 + "% keywords......");
		int stepcount=0;
		for (HashSet<String> keywords : topTopics.values()) {
			stepcount += keywords.size();
			consolidatedKeywds.addAll(keywords);
		}
		
		System.out.println("from total " + stepcount + ". Consolidated count=" + consolidatedKeywds.size());
System.out.println(consolidatedKeywds);
			
		driver = null;
		driver = new InvertedNdxDriver();
		driver.createAllIndex(authorListInputDir, stem);
		System.out.println("Original Unique Keywords in AuthorList: " + driver.invNdx.tokenHash.size());

		for (String topkeywd : consolidatedKeywds) {
			TokenInfo occurences = driver.getInvNdx().tokenHash.get(topkeywd);
			System.out.print(topkeywd);
			if (occurences != null) {
				 System.out.println(" (IDF=" + occurences.idf + ") occurs in:");
				for(TokenOccurrence occ : occurences.occList) {
					System.out.println("   " + occ.docRef.file.getName() + " " + occ.count + 
							   " times; |D|=" + occ.docRef.length);
				    }
			} else 
				System.out.println(" has no authorlist associated.");
		}
		System.out.println("\n---------------\n");
	//	driver.getInvNdx().print();
	}
	public final InvertedIndex getInvNdx() {
		return invNdx;
	}
	public final void setInvNdx(InvertedIndex invNdx) {
		this.invNdx = invNdx;
	}
	

}
