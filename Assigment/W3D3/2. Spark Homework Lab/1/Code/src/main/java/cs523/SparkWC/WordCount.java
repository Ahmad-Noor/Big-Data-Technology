package cs523.SparkWC;

/**
 * Hello world!
 *
 */

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"wordCount").setMaster("local"));

		int treshold = Integer.parseInt(args[2]);

		JavaRDD<String> lines = sc.textFile(args[0]);

		JavaPairRDD<String, Integer> counts = lines
				.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y);

		JavaPairRDD<String, Integer> wordsLessThanTreshold = counts
				.filter(w -> w._2 < treshold);

		JavaPairRDD<String, Integer> lettersCount = counts
				.filter(w -> w._2 >= treshold).map(w -> w._1)
				.flatMap(w -> Arrays.asList(w.split("")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y);

		counts.saveAsTextFile(args[1] + "/counts");
		wordsLessThanTreshold.saveAsTextFile(args[1] + "/wordsLessThanTreshold");
		lettersCount.saveAsTextFile(args[1] + "/lettersCount");

		sc.close();
	}
}
//
// import java.util.Arrays;
//
// import org.apache.spark.SparkConf;
// import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.JavaRDD;
// import org.apache.spark.api.java.JavaSparkContext;
//
// import scala.Tuple2;
//
// public class SparkWordCountjdk8
// {
// public static void main(String[] args) throws Exception
// {
// // Create a Java Spark Context
// JavaSparkContext sc = new JavaSparkContext(new
// SparkConf().setAppName("wordCount").setMaster("local"));
//
// // Load our input data
// JavaRDD<String> lines = sc.textFile(args[0]);
//
// // Calculate word count
// JavaPairRDD<String, Integer> counts = lines
// .flatMap(line -> Arrays.asList(line.split(" ")))
// .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
// .reduceByKey((x, y) -> x + y);
//
// // Save the word count back out to a text file, causing evaluation
// counts.saveAsTextFile(args[1]);
//
// sc.close();
// }
//
//
// // private static void wordCount(String fileName) {
// //
// // SparkConf sparkConf = new
// SparkConf().setMaster("local").setAppName("JD Word Counter");
// //
// // JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
// //
// // JavaRDD<String> inputFile = sparkContext.textFile(fileName);
// //
// // JavaRDD<String> wordsFromFile = inputFile.flatMap(content ->
// Arrays.asList(content.split(" ")));
// //
// // JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t,
// 1)).reduceByKey((x, y) -> (int) x + (int) y);
// //
// // countData.saveAsTextFile("CountData");
// // }
//
//
// }
