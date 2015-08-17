package org.dblp.topicmining;

/**
 * This class contains the constants used in this package
 *
 */
public class MalletConstants {

	public static final String ENCODING= "windows-1252";
	public static final boolean BINARYFEATURES= false;
	public static final String TOKENPATTERN= "\\p{Alpha}+";
	public static final int RANDOMSEED= 0;
	public static final int SHOWTOPICSINTERVAL= 50;
	public static final int TOPWORDS= 20;
	public static final boolean USESYMMETRICALPHA= true;
	public static final int OPTIMIZEINTERVAL= 0;
	public static final int OPTIMIZEBURNIN= 200;
	public static final int NUMTHREADS= 1;
	public static final double DOCTOPICSTHRESHOLD= 0.0;
	public static final int DOCTOPICSMAX= -1;
	public static final int MILLISECS_PER_DAY= 24*60*60*1000;
	public static final int FIRSTFEWWORDS = 5;
	
	// parameters to be optimized in LDA
	public static final double ALPHA= 0.01;
	public static final double BETA= 0.05;
	public static final int NUMITERATIONS= 1000;
	public static final int NUMTOPICS= 30;
	
	// paths to directories
	public static final String INPUTDIRPATH= ".\\input";
	public static final String OUTPUTDIRPATH= ".\\out";
	
	// input and output file names
	public static final String ALLMALLETFILENAME= "all.mallet";
	public static final String TRAININGMALLETFILENAME= "training.mallet";
	public static final String TESTINGMALLETFILENAME= "testing.mallet";
	public static final String evaluatorFile= "evaluator.mallet"; 
	public static final String TOPICKWFILENAME= "TopicsKeys.txt";
	public static final String TOPICSTATEFILENAME= "TopicsState.gz";
	public static final String TOPICDOCFILENAME= "TopicsDoc.txt";
	public static final String TOPICMODELFILENAME= "TopicsModel";
	
	
}
