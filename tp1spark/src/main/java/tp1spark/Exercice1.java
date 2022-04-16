package tp1spark;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class Exercice1 {
	private static final String NAMES_FILE = "names.csv";
	public static void runExo() throws IOException {
		SparkConf configuration = new SparkConf();
		// "local": execute task in local (One thread for all processes (workers)),
		// "local[3]": 3 threads for all the processes
		// "local[*]" : one thread for each process
		configuration.setAppName("exo1").setMaster("local");

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		/*
		// RDD from file

		JavaRDD<String> namesRdd = sparkContext.textFile(NAMES_FILE);
		JavaPairRDD<String, Integer> pairNamesRdd = namesRdd.mapToPair((name) -> new Tuple2<>(name, 1));
		 */

		// getting the file content as a list of String
		List<String> namesList = FileUtils.readLines(new File(NAMES_FILE), Charset.defaultCharset());
		// parallize : create a RDD fom a java List
		JavaRDD<String> rdd1 = sparkContext.parallelize(namesList);
		// filter: create a RDD that contains the names that start wit 'a' from rdd1
		// transformation
		JavaRDD<String> rdd3 = rdd1.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String name) throws Exception {
				// retrun the names that start with 'B';
				return name.startsWith("B") || name.startsWith("b") ;
			}
		});
		// get a java list from rdd2
		// collect: Action
		List<String> namesStartWithB = rdd3.collect();
		// show content
		System.out.println("RDD3: names start with 'B'");
		namesStartWithB.forEach((name) -> System.out.println(name));

		// rdd of all names of size > 5
		// transformation
		JavaRDD<String> rdd4 = rdd1.filter((name)-> name.length()>5);
		// displaying
		System.out.println("RDD4: names of size > 5");
		// action
		rdd4.foreach((name)-> System.out.println(name));

		//rdd with names contains 'o' character
		JavaRDD<String> rdd5 = rdd1.filter((name)-> name.contains("o"));
		// displaying
		System.out.println("RDD5: names contains 'o' character");
		// action
		rdd5.foreach((name)-> System.out.println(name));

		// rdd that contains the values of rdd3 and rdd4
		// transformation
		JavaRDD<String> rdd6 = rdd3.union(rdd4);
		System.out.println("RDD6: contains the values of rdd3 and rdd4");
		// action
		rdd6.foreach((name)-> System.out.println(name));

		// adding titles to names
		// if the name start with a char of ASCII value < (65+24)/2 add Mr.
		// if > add Mrs

		JavaRDD<String> rdd71 = rdd5.map((name)->{
			int center = 65+10;
			int firstCharAscii =  name.toString().toUpperCase().charAt(0);
			if(center < firstCharAscii) {
				return "Mr.".concat(name.toString());

			}
			else {
				return "Mrs.".concat(name.toString());
			}

		});

		System.out.println("RDD71: names with prefixes");
		// action
		rdd71.foreach((name)-> System.out.println(name));

		// rdd of lowercase name
		JavaRDD<String> rdd81 = rdd6.map((name)-> name.toString().toLowerCase());

		System.out.println("RDD81: names in lowercase");
		// action
		rdd81.foreach((name)-> System.out.println(name));


		// rdd of pairs (key, value)=(name, accur)
		// transformation
		JavaPairRDD<String, Integer> rdd72 = rdd71.mapToPair((name)-> new Tuple2(name, 1));

		System.out.println("RDD72: names in key value");
		// action
		rdd72.foreach((pair)-> System.out.println("("+pair._1()+", "+pair._2()+")"));

		// rdd of pairs (key, value)=(name, accur)
		JavaPairRDD<String, Integer> rdd82 = rdd81.mapToPair((name)-> new Tuple2(name, 1));

		System.out.println("RDD82: names in key value");
		// action
		rdd82.foreach((pair)-> System.out.println("("+pair._1()+", "+pair._2()+")"));


		// reducing by key
		// transformation
		JavaPairRDD<String, Integer> rdd7 = rdd72.reduceByKey((pairKey, sum)-> sum + pairKey);

		System.out.println("RDD7: wordcount");
		// action
		rdd7.foreach((pair)-> System.out.println("("+pair._1()+", "+pair._2()+")"));

		// reducing by key
		// transformation
		JavaPairRDD<String, Integer> rdd8 = rdd82.reduceByKey((pairKey, sum)-> sum + pairKey);

		System.out.println("RDD8: wordcount");
		// action
		rdd8.foreach((pair)-> System.out.println("("+pair._1()+", "+pair._2()+")"));


		// union
		// the rdd will contains non repeated words
		JavaRDD<String> rdd73 = rdd7.map((pair)-> pair._1.toLowerCase());
		JavaRDD<String> rdd83 = rdd8.map((pair)-> pair._1.toLowerCase());

		JavaRDD<String> rdd9 = rdd73.union(rdd83);
		System.out.println("RDD9: names");
		// action
		rdd9.foreach((name)-> System.out.println(name));

		// sorting alphabetically
		JavaRDD<String> rdd10 = rdd9.sortBy((name)-> name, true, 1);
		System.out.println("RDD9: names");
		// action
		rdd10.foreach((name)-> System.out.println(name));








	}
}
