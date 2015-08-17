package org.dblp.topicmining;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.regex.Pattern;

import org.dblp.dbextractor.DataExporterImporter;
import org.dblp.dbextractor.DataManager;

import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.FeatureSequence2AugmentableFeatureVector;
import cc.mallet.pipe.Input2CharSequence;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.PrintInputAndTarget;
import cc.mallet.pipe.SaveDataInSource;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.pipe.Target2Label;
import cc.mallet.pipe.TokenSequence2FeatureSequence;
import cc.mallet.pipe.TokenSequenceLowercase;
import cc.mallet.pipe.TokenSequenceRemoveStopwords;
import cc.mallet.pipe.iterator.FileIterator;
import cc.mallet.types.Instance;
import cc.mallet.types.InstanceList;

/**
 * 
 *
 */
public class PaperAbstractReader {

	// The abstracts read from the input file are saved as instances
	private InstanceList allInstances;
	private InstanceList trainingInstances;
	private InstanceList testingInstances;
	private int _domainId;
	
	public final void setDomainId(int id) {
		_domainId = id;
	}
	public final int getDomainId() {
		return _domainId;
	}
	public void readAbstractsFromFile(String inputFilePath, boolean keepSequence, boolean printOutput){
		
		String targetName= "";
		File commentsFile= new File(inputFilePath);
		Pipe instancePipe;
			
		// Create a list of pipes that will be added to a SerialPipes object later
		ArrayList<Pipe> pipeList = new ArrayList<Pipe>();

		// Convert the "target" object into a numeric index
		//  into a LabelAlphabet.
		pipeList.add(new Target2Label());

		// The "data" field is currently a filename. Save it as "source".
		pipeList.add( new SaveDataInSource() );

		// Set "data" to the file's contents. "data" is now a String.
		pipeList.add( new Input2CharSequence(TopicConstants.ENCODING) );
		
		// Add the tokenizer
		Pattern tokenPattern = Pattern.compile(TopicConstants.TOKENPATTERN);	
		pipeList.add(new CharSequence2TokenSequence(tokenPattern));		
		pipeList.add(new TokenSequenceLowercase());

		TokenSequenceRemoveStopwords stopwordFilter =
			new TokenSequenceRemoveStopwords(false, false);
		pipeList.add(stopwordFilter);

		pipeList.add( new TokenSequence2FeatureSequence() );
		
		// For many applications, we do not need to preserve the sequence of features,
		//  only the number of times times a feature occurs.
		if (!keepSequence) {
			pipeList.add( new FeatureSequence2AugmentableFeatureVector(TopicConstants.BINARYFEATURES) );
		}

		if (printOutput) {
			pipeList.add(new PrintInputAndTarget());
		}

		instancePipe = new SerialPipes(pipeList);
		
		allInstances = new InstanceList (instancePipe);
		
		allInstances.addThruPipe(new Instance (commentsFile, targetName, commentsFile.toURI(), null));
		
	}
	public int[] readAbstractsFromDatabase(DataManager d, String basePath, boolean keepSequence, boolean printOutput) throws SQLException {
		Connection conn = d.getConnection();
		int[] years= d.getMinMaxYearForResearchDomain(_domainId);
		String dirPath = basePath ; //+ File.separator + "papers";
		DataExporterImporter exporter = new DataExporterImporter();
		int[] counts= exporter.exportPaperAbstracts(conn, _domainId, years[0], years[1], false, dirPath); // Do stemming and stopword removal while dumping
		System.out.println("Total " + counts[0] + " papers with " + (counts[0]-counts[1]) + 
				" abstracts have been exported to " + dirPath);
		readAbstractsFromDirectory(dirPath, keepSequence, printOutput); 
		return counts;
	}
	public void readAbstractsFromDirectory(String inputDirectoryPath, boolean keepSequence, boolean printOutput){
		
		File[] directories= null;
		Pipe instancePipe;
		boolean removeCommonPrefix = true;
		int commonPrefixIndex = inputDirectoryPath.length();

		directories = new File[1];
		directories[0] = new File (inputDirectoryPath);
		System.out.println("Labels =    "+inputDirectoryPath);

		// Create a list of pipes that will be added to a SerialPipes object later
		ArrayList<Pipe> pipeList = new ArrayList<Pipe>();
		// Convert the "target" object into a numeric index into a LabelAlphabet.
		// The "data" field is currently a filename. Save it as "source".
		// Set "data" to the file's contents. "data" is now a String.
		pipeList.add(new Target2Label());
		pipeList.add( new SaveDataInSource() );
		pipeList.add( new Input2CharSequence(TopicConstants.ENCODING) );
		
		// Add the tokenizer. Read only defined tokens from the file.
		Pattern tokenPattern = Pattern.compile(TopicConstants.TOKENPATTERN);	
		pipeList.add(new CharSequence2TokenSequence(tokenPattern));
		pipeList.add(new TokenSequenceLowercase());
		
		TokenSequenceRemoveStopwords stopwordFilter =
			new TokenSequenceRemoveStopwords(false, true);
		stopwordFilter.addStopWords(new File("config"+File.separator+"stopwords.txt"));
		pipeList.add(stopwordFilter);
		pipeList.add( new TokenSequence2FeatureSequence() );

		// For many applications, we do not need to preserve the sequence of features,
		//  only the number of times times a feature occurs.
		if (!keepSequence) {
			pipeList.add( new FeatureSequence2AugmentableFeatureVector(TopicConstants.BINARYFEATURES) );
		}
		if (printOutput) {
			pipeList.add(new PrintInputAndTarget());
		}
		instancePipe = new SerialPipes(pipeList);
		allInstances = new InstanceList (instancePipe);
		allInstances.addThruPipe (new FileIterator (directories, FileIterator.STARTING_DIRECTORIES, removeCommonPrefix));
	}
	
	public void writeInstancesToMalletFile(String folderPath) throws IOException{
		
		if(allInstances.size() > 0){
			ObjectOutputStream oos = 
				new ObjectOutputStream(new FileOutputStream(folderPath+"\\"+TopicConstants.ALLMALLETFILENAME));
			oos.writeObject(allInstances);
			oos.close();
		}
		if(trainingInstances.size() > 0){
			ObjectOutputStream oos = 
				new ObjectOutputStream(new FileOutputStream(folderPath+"\\"+TopicConstants.TRAININGMALLETFILENAME));
			oos.writeObject(trainingInstances);
			oos.close();
		}
		if(testingInstances.size() > 0){
			ObjectOutputStream oos = 
				new ObjectOutputStream(new FileOutputStream(folderPath+"\\"+TopicConstants.TESTINGMALLETFILENAME));
			oos.writeObject(testingInstances);
			oos.close();
		}
	}
	
	public void splitInstances(double traningProportion, double validationProportion, String folderPath){
		Random r= new Random ();
		// Split into three lists...
		InstanceList[] instanceLists = allInstances.split (r, new double[] {traningProportion, 1-traningProportion-validationProportion, validationProportion});

		if (instanceLists[0].size() > 0)
			trainingInstances= instanceLists[0];
		if (instanceLists[1].size() > 0)
			testingInstances= instanceLists[1];
		//if (instanceLists[2].size() > 0)
			//instanceLists[2].save(new File(folderPath+"\\validationComments.mallet" ));
	}
	
	public InstanceList getAllInstances(){
		return allInstances;
	}
	
	public InstanceList getTrainingInstances(){
		return trainingInstances;
	}
	
	public InstanceList getTestingInstances(){
		return testingInstances;
	}
}
