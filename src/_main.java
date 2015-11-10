
import java.io.IOException;

public class _main {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String input = "input/";
		String output = "output/";
		
//		String input = args[0];
//		String output = args[1];
		
		// TF-IDF
		TFIDF tfIdf = new TFIDF(input, output);
		tfIdf.calculate();
		
		// K-Means
		KMeans kmeans = new KMeans(
				output + TFIDF.TFIDF_DIR + "/tfidf-vectors/part-r-00000",
				output + "kmeans/");
		kmeans.initialize(10);
		kmeans.run();
		
		// output result
		SequenceFileUtils.outputClusterFile(
				output + "kmeans/output/" + SequenceFileUtils.getFinalIterName(output + "kmeans/output") + "/part-r-00000", 
				output + "result.txt");
		
	}

}
