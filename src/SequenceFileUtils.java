import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;

public class SequenceFileUtils {
	
	private static Configuration _CONFIGURATION = new Configuration();
	
	public void text2Sequence(String input, String output) throws IOException {
		File inputFolder = new File(input);
		File[] files = inputFolder.listFiles();
		
		
//		SequenceFile.Writer sequenceWriter = SequenceFile.createWriter(
//				_CONFIGURATION, 
//				SequenceFile.Writer.file(new Path(output)),
//				SequenceFile.Writer.keyClass(Text.class),
//				SequenceFile.Writer.valueClass(Text.class));
		
		SequenceFile.Writer sequenceWriter = new SequenceFile.Writer(
				FileSystem.get(_CONFIGURATION), _CONFIGURATION, new Path(output), Text.class, Text.class);
		for(File f : files) {
			sequenceWriter.append(
					new Text(f.getName()), 
					new Text(FileUtils.readFileToString(f)));
		}
		sequenceWriter.close();
	}
	
	public void sequence2Text(String input, String output) throws IOException {
		SequenceFileIterable<Writable, Writable> iterable = new SequenceFileIterable<Writable, Writable>(
                new Path(input), _CONFIGURATION);
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(output)));
		for(Pair<Writable, Writable> pair : iterable) {
			bufferedWriter.write(pair.getFirst() + "\t" + pair.getSecond() + "\n");
		}
		bufferedWriter.close();
	}
	
    public static void printSequenceFile(String file) {
        SequenceFileIterable<Writable, Writable> iterable = 
        		new SequenceFileIterable<Writable, Writable>(new Path(file), _CONFIGURATION);
        for(Pair<Writable, Writable> pair : iterable) {
            System.out.format("%10s -> %s\n", pair.getFirst(), pair.getSecond());
        }
    }
    
    public static void outputClusterFile(String fileName, String output) throws IOException {
    	SequenceFileIterable<Writable, ClusterWritable> iterable = 
    			new SequenceFileIterable<Writable, ClusterWritable>(new Path(fileName), _CONFIGURATION);
    	BufferedWriter writer = new BufferedWriter(new FileWriter(new File(output)));
    	for(Pair<Writable, ClusterWritable> pair : iterable)
    		writer.write(
    				pair.getFirst().toString() 
    				+ "\t"
    				+ pair.getSecond().getValue().getCenter().toString()
    				+ "\n");
    	writer.close();
    }
    
    public static String getFinalIterName(String folderName) {
    	File folder = new File(folderName);
    	File[] files = folder.listFiles();
    	for(File f : files) {
    		if(f.getName().endsWith("-final"))
    			return(f.getName());
    	}
    	return("Not Found");
    }
}
