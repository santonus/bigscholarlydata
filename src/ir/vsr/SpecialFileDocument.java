package ir.vsr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;


public class SpecialFileDocument extends TextFileDocument {

	protected static HashSet<String> glossaryWords = null;
	protected final String glossaryWordsFile = "config/domainglossary.txt";
	public	SpecialFileDocument(File file, boolean stem) {
		super(file,stem);
		if (glossaryWords== null)
			loadGlossary();
	}
	@Override
	public HashMapVector hashMapVector() {
		HashMapVector bagOfWords= super.hashMapVector();
		for (String word : glossaryWords) {
			if (bagOfWords.hashMap.containsKey(word))
				bagOfWords.increment(word, 2);
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
