package cs523.SparkWC;

import java.text.Collator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkExtraCredit {

	public static void main(String[] args) {

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"wordCount").setMaster("local"));
		
		int moreThan = Integer.parseInt(args[2]);
		
		LogParser logParser = new LogParser();

		JavaRDD<LogParser> lines = sc.textFile(args[0]).map(
				l -> logParser.parseLog(l));

		JavaPairRDD<String, Integer> counts = lines
				.mapToPair(
						w -> new Tuple2<String, Integer>(w.getIpAddress(), 1))
				.reduceByKey((x, y) -> x + y).filter(i -> i._2 > moreThan)
				.reduceByKey((f, t) -> f);

		counts.saveAsTextFile(args[1]);

		sc.close();

	}

}
