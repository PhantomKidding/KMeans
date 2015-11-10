import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

public class KMeans {
	
	private static Configuration _CONFIGURATION = new Configuration();
	
	private static String _INPUT_DIR;
	private static String _OUTPUT_DIR;
	public static String KMEANS_CLUSTER_DIR = "output/";
	private static int _K; // number of centroids
	
	public KMeans() {
		_INPUT_DIR = "output/tfidf/tfidf-vectors/part-r-00000";
		_OUTPUT_DIR = "output/kmeans/";
	}
	
	public KMeans(String input, String output) {
		_INPUT_DIR = input;
		_OUTPUT_DIR = output;
	}
	
	public void initialize(int k) throws IOException {
		_K = k;
		RandomSeedGenerator.buildRandom(
				_CONFIGURATION, 
				new Path(_INPUT_DIR), 
				new Path(_OUTPUT_DIR + "cluster_init"), 
				_K, 
				new EuclideanDistanceMeasure());
	}
	
	public void run() throws ClassNotFoundException, IOException, InterruptedException {
		KMeansDriver.run(
				_CONFIGURATION,
				new Path(_INPUT_DIR),
				new Path(_OUTPUT_DIR + "cluster_init/part-randomSeed"), 
				new Path(_OUTPUT_DIR + KMEANS_CLUSTER_DIR), 
				0.001,			// convergenceDelta
				10,				// maxIter
				true,			// runClustering
				0,				// clusterClassificationThreshold: 
								// a clustering strictness / outlier removal parameter
				false);			// runSequential
	}
}
