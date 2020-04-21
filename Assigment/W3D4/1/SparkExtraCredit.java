package cs523.SparkWC;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.typesafe.config.ConfigException.Parse;

import scala.Tuple2;

public class SparkExtraCredit {

	public static final int NUM_FIELDS = 9;

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(new Path(args[1]))) {

			fs.delete(new Path(args[1]), true);
		}

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"wordCount").setMaster("local"));

		LogParser logParser = new LogParser();

		Date dateFrom = new SimpleDateFormat("dd/MMM/yyyy").parse(args[2]);
		Date dateTo = new SimpleDateFormat("dd/MMM/yyyy").parse(args[3]);

		// Load our input data
		JavaRDD<LogParser> lines = sc.textFile(args[0]).map(
				l -> logParser.parseLog(l));

		JavaPairRDD<String, Integer> counts = lines
				.filter(f -> f.getResponseCode().equals("401")
						&& ((f.getDateTime().after(dateFrom) && f.getDateTime()
								.before(dateTo))
								|| f.getDateTime().equals(dateTo) || f
								.getDateTime().equals(dateFrom)))
				.mapToPair(
						w -> new Tuple2<String, Integer>(w.getResponseCode(), 1))
				.reduceByKey((x, y) -> x + y);

		System.out.println(counts.count());

		counts.saveAsTextFile(args[1]);

		sc.close();

	}

}
