package org.dblp.topicmining;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import cc.mallet.types.InstanceList;


/**
 * This class has some utilities that can be used as re-usable methods 
 *
 */
public class Utilities {
	
	public static void writeInstancesToMalletFile(InstanceList instances, String filePath) throws FileNotFoundException, IOException{
		ObjectOutputStream oos = 
			new ObjectOutputStream(new FileOutputStream(filePath));
		oos.writeObject(instances);
		oos.close();
	}

	public static InstanceList readInstancesFromMalletFile(String filePath) throws FileNotFoundException, IOException, ClassNotFoundException{
		ObjectInputStream ois= new ObjectInputStream(new FileInputStream(filePath));
		return (InstanceList)ois.readObject();
	}
}
