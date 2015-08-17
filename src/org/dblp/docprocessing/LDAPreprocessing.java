package org.dblp.docprocessing;

import org.apache.mahout.driver.MahoutDriver;
import org.dblp.model.BibEntry;
import org.apache.mahout.text.SequenceFilesFromDirectory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

public class LDAPreprocessing {
	private String _appBase;
	private String _inputCorpusDir;
	private String _sparseVectorDir;
	private String _ldaOutputDir;
	private String _topicOutputDir;
	
	public final String getAppBase() {
		return _appBase;
	}
	public final void setAppBase(String appBase) {
		_appBase = appBase;
	}
	public final String getInputCorpusDir() {
		return _inputCorpusDir;
	}
	public final void setInputCorpusDir(String inputCorpusDir) {
		_inputCorpusDir = inputCorpusDir;
	}
	public final String getSparseVectorDir() {
		return _sparseVectorDir;
	}
	public final void setSparseVectorDir(String sparseVectorDir) {
		_sparseVectorDir = sparseVectorDir;
	}
	public final String getLDAOutputDir() {
		return _ldaOutputDir;
	}
	public final void setLDAOutputDir(String ldaOutputDir) {
		_ldaOutputDir = ldaOutputDir;
	}
	public final String getTopicOutputDir() {
		return _topicOutputDir;
	}
	public final void setTopicOutputDir(String topicOutputDir) {
		this._topicOutputDir = topicOutputDir;
	}
	public LDAPreprocessing() {
	}
/*	public void dumpData() {
		String corpusBase= _appBase + File.separator + _inputCorpusDir;
		(new File(corpusBase)).mkdirs();
		try {
			for (int i=0; i < _data.length; i++) {
				String filename= corpusBase + File.separator + _data[i].getEntryId() + ".txt";
				PrintWriter writer = new PrintWriter(new FileOutputStream(filename));
				writer.println(_data[i].getTitleAsBOW());
				writer.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}*/
	/** Currently it is not working from Windows OS **/
	public void runMahout() {
		String seqDir = _appBase + File.separator ;//+ _seqFileDir;
		String sparseVectorDir = _appBase + File.separator + _sparseVectorDir;
		String ldaInputDir = sparseVectorDir + File.separator + "tf-vectors";
		String ldaOutputDir = _appBase + File.separator + _ldaOutputDir;
		String topicInputDir = ldaOutputDir + File.separator + "state-20";
		String dictDir = sparseVectorDir + File.separator + "dictionary.file-0";
		String topicOutputDir = _appBase + File.separator + _topicOutputDir;
		
		String [] seqArgs = {"seqdirectory ", "-i ", _appBase + File.separator + _inputCorpusDir,
				"-o ", seqDir, "-c ", "UTF-8 ", "-chunk ", "5"};
		String [] sparseArgs = {"seq2sparse", "-i", seqDir,
				"-o", sparseVectorDir, "-wt", "tf", "-seq", "-nr", "3"};
		String [] ldaArgs = {"lda", "-i", ldaInputDir,
				"-o", ldaOutputDir, "-k", "20", "-v", "50000", "-ow", "-x", "20"};
		String [] topicArgs = {"seqdirectory", "-i", topicInputDir,
				"-o", topicOutputDir, "-d", dictDir, "-dt", "sequencefile"};

		try {
//			SequenceFilesFromDirectory seqFiles= new SequenceFilesFromDirectory();
//			seqFiles.main(args);
			System.out.println("bin/mahout " + seqArgs[0] );
//			MahoutDriver.main(seqArgs);
			System.out.println("bin/mahout " + sparseArgs);
//			MahoutDriver.main(sparseArgs);
			System.out.println("bin/mahout " + ldaArgs);;
//			MahoutDriver.main(ldaArgs);
			System.out.println("bin/mahout " + topicArgs);
//			MahoutDriver.main(topicArgs);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
