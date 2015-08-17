package org.textanalysis.ir;

import ir.vsr.HashMapVector;
import ir.vsr.TextStringDocument;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
/** It inherits TextStringDocument. In addition to TextStringDocument functionality, it does two important things. 
* 1. It always use stemming capability.
* 2. It loads stopwords by stemming every stopword.
* 3. It also loads another file called domainglossary, that contains domain specific, important words.
* 4. It bumps the weight of those words which are in domainglossary and also present in the current bag of words. 
* @author Santonu Sarkar
*/
public class SpecialTextStringDocument extends TextStringDocument {
	protected static HashSet<String> glossaryWords = null;
	protected final String glossaryWordsFile = "config/domainglossary.txt";
	public	SpecialTextStringDocument(String doc) {
		super(doc,true);
		if (glossaryWords== null)
			loadGlossary();
	}
	@Override
	protected String processword(String word) {
		String stemmedstopword = stemmer.stripAffixes(word);
		return stemmedstopword;
	} 
	@Override
	public HashMapVector hashMapVector() {
		HashMapVector bagOfWords= super.hashMapVector();
		for (String word : glossaryWords) {
			if (bagOfWords.hashMap.containsKey(word))
				bagOfWords.increment(word, 1);
		}
		return bagOfWords;
	}
	@Override
	protected boolean allLetters(String token) {
		for (int i = 0; i < token.length(); i++) {
			char ch= token.charAt(i);
			if (!Character.isLetter(ch) && ch != '-')
				return false;
		}
		return true;
	}
	protected void loadGlossary() {
		glossaryWords = new HashSet<String>(20);
		String line;
		try {
			// Open glossary file for reading
			BufferedReader in = new BufferedReader(
					new FileReader(glossaryWordsFile));
			// Read in glossary words, one per line, until file is empty
			while ((line = in.readLine()) != null) {
				// Index word into the hashtable with
				// the default empty string as a "dummy" value.
				glossaryWords.add(processword(line));
			}
			in.close();
		} catch (IOException e) {
			System.out.println("\nCould not load glossary file: "
					+ glossaryWordsFile);
			System.exit(1);
		}
	}
}
