package exercice3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Exercice3 {
	private static String DATA_1750_CSV_FILE = "1750.csv";
	public static void runProccess() {
		SparkConf configuration = new SparkConf();
		// "local": execute task in local (One thread for all processes (workers)),
		// "local[3]": 3 threads for all the processes
		// "local[*]" : one thread for each process
		configuration.setAppName("exo1").setMaster("local");

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);

		// getting file to RDD
		JavaRDD<String> fileLinesRDD = sparkContext.textFile(DATA_1750_CSV_FILE);

		// les valeures temp minimales
		JavaPairRDD<String,Double> tempsTypePairs = fileLinesRDD.mapToPair((line)->{
			String [] lineArr = line.split(",");
			return new Tuple2<String, Double>(lineArr[2], Double.valueOf(lineArr[3]));
		});

		/*
		 *  Température minimale moyenne.
			• Température maximale moyenne.

		 */

		// regouper les temps par type

		JavaPairRDD<String, Double> reducingByType = tempsTypePairs.reduceByKey((temp, sum)-> sum+temp);

		//		JavaPairRDD<String, Double> meansTypesPaies = reducingByType.map((pair)->);

		// to find the number of each type should create an RDD for each one

		JavaPairRDD<String, Double> minTypeValues =  tempsTypePairs.filter((pair) -> pair._1().contains("TMIN"));
		JavaPairRDD<String, Double> maxTypeValues =  tempsTypePairs.filter((pair) -> pair._1().contains("TMAX"));

		int tempMinNumber = minTypeValues.collect().size();
		int tempMaxNumber = maxTypeValues.collect().size();

		// sums
		JavaPairRDD<String, Double> minTempSum = minTypeValues.reduceByKey((temp, sum)-> sum + temp);
		JavaPairRDD<String, Double> maxTempSum = maxTypeValues.reduceByKey((temp, sum)-> sum + temp);

		// means
		double minTypeMean = minTempSum.collect().get(0)._2 / tempMinNumber;
		double maxTypeMean = maxTempSum.collect().get(0)._2 / tempMaxNumber;

		System.out.println("Température minimale moyenne: "+ minTypeMean + "\n");
		System.out.println("Température maximale moyenne: "+ maxTypeMean + "\n");



		/*
		 * • Température maximale la plus élevée.
			• Température minimale la plus basse.

		 */

		Double greaterMaxTemp = Double.MAX_VALUE , lowerMinTemp = Double.MIN_VALUE;
		//		minTypeValues.foreach((pair)-> {
		//			if(pair._2 < lowerMinTemp) {
		//				lowerMinTemp = pair._2;
		//			}
		//		});

		JavaRDD<Double> sortedMinTypeValues = minTypeValues.map((pair)-> pair._2).sortBy(temp->temp, true, 0);
		JavaRDD<Double> sortedMaxTypeValues = maxTypeValues.map((pair)-> pair._2).sortBy(temp->temp, false, 0);


		System.out.println("Température maximale la plus élevée: "+ sortedMaxTypeValues.collect().get(0) +"\n");
		System.out.println("Température minimale la plus basse: "+ sortedMinTypeValues.collect().get(0) +"\n");


		/*
		 * • Top 5 des stations météo les plus chaudes.
			• Top 5 des stations météo les plus froides.
		 */

		

		// goupe by stations > calculating means by stations > sort rdd -> get the first 5 stations names

		JavaPairRDD<String, Double> stationTempPairs= fileLinesRDD.mapToPair((line)->{
			String [] lineArr = line.split(",");
			return new Tuple2<String, Double>(lineArr[0], Double.valueOf(lineArr[3]));
		});

		JavaPairRDD<String, Double> groupByStationsTemp = stationTempPairs.reduceByKey((temp, sum)-> sum+temp);

		JavaRDD<String> satationsNames = groupByStationsTemp.map((pair)-> pair._1);
		List<String> stationList = satationsNames.collect();
		//Map<String, Double> stationMeanPairs = new HashMap<>();
		List<Tuple2<String, Double>> stationMeanPairsList = new ArrayList<>();

		// Estracting StationName, stationMean pairs
		for(String station: stationList) {
			JavaPairRDD<String, Double> stationValue = stationTempPairs.filter((pair)-> pair._1.equals(station));
			JavaPairRDD<String, Double> stationSum = stationTempPairs.reduceByKey((temp, sum)-> temp+sum);
			//stationMeanPairs.put(station, stationSum.collect().get(0)._2/stationValue.collect().size());///sum/Number of stations
			stationMeanPairsList.add(new Tuple2(station, stationSum.collect().get(0)._2/stationValue.collect().size()));///sum//Number of stations
	}

	//

	for(String station: stationMeanPairs.keySet()) {
		System.out.println(station);
	}
	/

	JavaPairRDD<String, Double> stationMeanPairsRDD = sparkContext.parallelizePairs(stationMeanPairsList);
	// swap to meanTemp, Station
	JavaPairRDD<Double, String> stationMeanPairsSwappedRDD = stationMeanPairsRDD.mapToPair((pair) -> {
		return new Tuple2(pair._2, pair._1);
	});

	// sorted
	JavaPairRDD<Double, String> stationMeanPairsSortedRDD = stationMeanPairsSwappedRDD.sortByKey();

	// collect and print
	List<Tuple2<Double, String>> stationMeanPairsSortedList  = stationMeanPairsSortedRDD.collect();
	List<Tuple2<Double, String>> most5HotStations = stationMeanPairsSortedList.subList(stationMeanPairsSortedList.size()-6, stationMeanPairsSortedList.size()-1);
	List<Tuple2<Double, String>> most5ColdStations = stationMeanPairsSortedList.subList(0, 5);


	System.out.println("• Top 5 des stations météo les plus chaudes.");
	for(Tuple2<Double, String> pair: most5HotStations) {
		System.out.println(pair._2+ " "+ pair._1);
	}

	System.out.println("• Top 5 des stations météo les plus froides.");
	for(Tuple2<Double, String> pair: most5ColdStations) {
		System.out.println(pair._2+ " "+ pair._1);
	}

		 

	}
}
