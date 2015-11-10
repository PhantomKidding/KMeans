import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.mahout.common.Pair;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

public class TFIDF {

	private static Configuration _CONFIGURATION;
	private static FileSystem _FILESYSTEM;
	
	private static String _INPUT_DIR;
	private static String _OUTPUT_DIR;
	public static String SEQUENCE_FILE = "sequence";
	public static String TFIDF_DIR = "tfidf/";
	
	public TFIDF() throws IOException {
		_CONFIGURATION = new Configuration();
		_FILESYSTEM = FileSystem.get(_CONFIGURATION);
		
		_INPUT_DIR = "input/";
		_OUTPUT_DIR = "output/";
		
		text2Sequence();
	}
	
	public TFIDF(String input, String output) throws IOException {
		_CONFIGURATION = new Configuration();
		_FILESYSTEM = FileSystem.get(_CONFIGURATION);
		
		_INPUT_DIR = input;
		_OUTPUT_DIR = output;
		
		text2Sequence();
	}
	
	public void calculate() throws ClassNotFoundException, IOException, InterruptedException {
		
		boolean sequential = false;
        boolean named = false;
		
		
		DocumentProcessor.tokenizeDocuments(
				new Path(_OUTPUT_DIR, SEQUENCE_FILE),
				StandardAnalyzer.class,
				new Path(_OUTPUT_DIR, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER), 
				_CONFIGURATION);
		
		DictionaryVectorizer.createTermFrequencyVectors(
				new Path(_OUTPUT_DIR, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER), 
				new Path(_OUTPUT_DIR), 
				DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER, 
				_CONFIGURATION, 
				1, 				// minimum frequency for each corpus
				1, 				// n gram
				0.0f, 			// min value of LLR to used to prune ngrams
				PartialVectorMerger.NO_NORMALIZING, // normPower L_p norm computed
				true, 			// log normalize
				1, 				// numReducers
				100, 			// chunk size megabytes
				sequential, 
				named);
		
		Pair<Long[], List<Path>> documentFrequencies = TFIDFConverter.calculateDF(
				new Path(_OUTPUT_DIR, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER), 
				new Path(_OUTPUT_DIR, TFIDF_DIR),
				_CONFIGURATION, 
                100);
		
		TFIDFConverter.processTfIdf(
				new Path(_OUTPUT_DIR, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER), 
				new Path(_OUTPUT_DIR, TFIDF_DIR),
				_CONFIGURATION, 
                documentFrequencies, 
                1, 				// minimum document frequency (percentage)
                100,			// maximum document frequency (percentage)
                PartialVectorMerger.NO_NORMALIZING, 
                false, 			// log normalize
                sequential, 			
                named, 
                1);				// numReducers
	}
    
	public void text2Sequence() throws IOException {
		File inputFolder = new File(_INPUT_DIR);
		File[] files = inputFolder.listFiles();

//		SequenceFile.Writer sequenceWriter = SequenceFile.createWriter(
//				_CONFIGURATION,
//				SequenceFile.Writer.file(new Path(_OUTPUT_DIR, SEQUENCE_FILE)),
//				SequenceFile.Writer.keyClass(Text.class),
//				SequenceFile.Writer.valueClass(Text.class));
		SequenceFile.Writer sequenceWriter = new SequenceFile.Writer(
				_FILESYSTEM, _CONFIGURATION, new Path(_OUTPUT_DIR, SEQUENCE_FILE), Text.class, Text.class);
		for(File f : files) {
			sequenceWriter.append(
					new Text(f.getName()), 
					new Text(FileUtils.readFileToString(f)));
		}
		sequenceWriter.close();
	}
	
	public static void setSequenceFileName(String name) {
		SEQUENCE_FILE = name;
	}
	
    public Configuration getConf() {
    	return _CONFIGURATION;
    }
}
