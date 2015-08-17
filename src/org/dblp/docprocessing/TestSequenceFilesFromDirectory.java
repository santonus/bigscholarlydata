/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dblp.docprocessing;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.junit.Before;
import org.junit.Test;

public final class TestSequenceFilesFromDirectory {

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final String[][] DATA1 = {
      {"test1", "This is the first text."},
      {"test2", "This is the second text."},
      {"test3", "This is the third text."}
  };

  public void createSeqFile(String inputDir, String outputDir) {
	  Configuration conf = new Configuration();
	  String prefix = "UID";
	  try {
		FileSystem fs = FileSystem.get(conf);
		SequenceFilesFromDirectory.main(new String[] {"--input",
				  inputDir, "--output", outputDir, "--chunkSize",
				  "64", "--charset", UTF8.displayName(Locale.ENGLISH), "--keyPrefix", prefix});
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  /** Story converting text files to SequenceFile */
  @Test
  public void testSequnceFileFromTsvBasic(String inputDir, String outputDir) throws Exception {
    // parameters
    Configuration conf = new Configuration();
    
    FileSystem fs = FileSystem.get(conf);
    
    // create
    (new File(inputDir)).mkdirs();
    
    // prepare input files
    createFilesFromArrays(conf, inputDir, DATA1);
    createSeqFile(inputDir, outputDir);
    String prefix = "UID";
    
    // check output chunk files
    checkChunkFiles(conf, outputDir, DATA1, prefix);
  }

  private static void createFilesFromArrays(Configuration conf, String inputDir, String[][] data) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    for (String[] aData : data) {
      OutputStreamWriter osw = new OutputStreamWriter(fs.create(new Path(inputDir, aData[0])), UTF8);
      osw.write(aData[1]);
      osw.close();
    }
  }

  private static void checkChunkFiles(Configuration conf, String outputDirStr, String[][] data, String prefix)
    throws IOException, InstantiationException, IllegalAccessException {
    FileSystem fs = FileSystem.get(conf);
    
    // output exists?
    Path outputDir = new Path(outputDirStr);
    FileStatus[] fstats = fs.listStatus(outputDir, new ExcludeDotFiles());
    assert (fstats.length == 1);
    assert (fstats[0].getPath().getName().equals("chunk-0"));
    // read a chunk to check content
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, fstats[0].getPath(), conf);
    assert(reader.getKeyClassName().equals("org.apache.hadoop.io.Text"));
    assert(reader.getValueClassName().equals("org.apache.hadoop.io.Text"));
    Writable key = reader.getKeyClass().asSubclass(Writable.class).newInstance();
    Writable value = reader.getValueClass().asSubclass(Writable.class).newInstance();
    
    Map<String,String> fileToData = new HashMap<String,String>();
    for (String[] aData : data) {
      fileToData.put(prefix + Path.SEPARATOR + aData[0], aData[1]);
    }

    for (String[] aData : data) {
      assert(reader.next(key, value));
      String retrievedData = fileToData.get(key.toString().trim());
      assert (retrievedData != null);
      assert(retrievedData.equals(value.toString().trim()));
    }
    reader.close();
  }
  
  /**
   * exclude hidden (starting with dot) files
   */
  private static class ExcludeDotFiles implements PathFilter {
    @Override
    public boolean accept(Path file) {
      return !file.getName().startsWith(".");
    }
  }

}

