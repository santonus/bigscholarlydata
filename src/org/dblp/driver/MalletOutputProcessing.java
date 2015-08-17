package org.dblp.driver;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;

public class MalletOutputProcessing {
	public static void main(String[] args) throws Exception {
		BufferedReader in = new BufferedReader(
				new FileReader(args[0]));
		String opfilename= args[0] + ".csv";
		PrintWriter writer = new PrintWriter(new FileOutputStream(opfilename));
		// Read in glossary words, one per line, until file is empty
		String line;
		int numTopics= Integer.valueOf(args[1]);
		int topCount= numTopics;
		if (args.length ==3)
			topCount= Integer.valueOf(args[2]);
		writer.print("-");
		for (int i=0; i <numTopics; i++) {
			writer.print(",T" + i);
		}
		writer.println();
		while ((line = in.readLine()) != null) {
			String[] fields= line.split(" ");
			String currentTopicProbability[]= new String[numTopics]; // don't have time to find out initialization API
			for (int i=2, j=0;i < fields.length; i=i+2,j++) {
				int topicNdx= Integer.valueOf(fields[i]);
				if (topicNdx >= numTopics)
					throw new Exception("Topic Index out of range for line=" + line +
							"\n topicNdx value=" + topicNdx);
				if (j < topCount )
					currentTopicProbability[topicNdx]=fields[i+1];
				else 
					currentTopicProbability[topicNdx]="0";
			}
			writer.print(fields[1]);
			for (int i=0; i <numTopics; i++) {
				writer.print("," + currentTopicProbability[i]);
			}
			writer.println();
		}
		writer.close();
	}
}
