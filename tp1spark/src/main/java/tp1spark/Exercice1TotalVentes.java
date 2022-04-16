package tp1spark;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Exercice1TotalVentes {

	private static JavaSparkContext initSparkContext() {
		SparkConf configuration = new SparkConf();
		// "local": execute task in local (One thread for all processes (workers)),
		// "local[3]": 3 threads for all the processes
		// "local[*]" : one thread for each process
		configuration.setAppName("exo1").setMaster("local");

		JavaSparkContext sparkContext = new JavaSparkContext(configuration);
		return sparkContext;
	}
	public static void runExo1(String inputFile,String outputFile) throws IOException {
		/*
		 une application Spark permettant, à partir d’un
		fichier texte (ventes.txt) en entré, contenant les ventes d’une entreprise dans
		les différentes villes, de déterminer le total des ventes par ville. La structure
		du fichier ventes.txt est de la forme suivante :
		 date ville produit prix
		 */

		JavaSparkContext sparkContext = initSparkContext();

		// 1: get rdd from inputfile
		if(inputFile == null || !(new File(inputFile)).exists()) throw new RuntimeException("Input file invalid or does not exist");
		JavaRDD<String> ventesFileContent = sparkContext.textFile(inputFile);
		// remove file header
		JavaRDD<String> ventes = ventesFileContent.filter((venteLine)-> {
			return !venteLine.contains("ville");
		});
		// headers
		JavaRDD<String> fileHeaderRDD = ventesFileContent.filter((venteLine)-> {
			return venteLine.contains("ville") && venteLine.contains("prix");
		});

		//creating pair RDD<ville, prix>
		JavaPairRDD<String, Double> ventesPairs = ventes.mapToPair((venteline)->{
			Tuple2<String, Double> ventePrixTuple;
			String [] venteLineStrs = venteline.split(",");
			String ville = venteLineStrs[1];
			Double prix = Double.valueOf(venteLineStrs[3]);
			ventePrixTuple = new Tuple2(ville,prix );
			return ventePrixTuple;
		});

		// reduce by the 'ville' and calculate the sum of ventes
		JavaPairRDD<String, Double> ventesByVille = ventesPairs.reduceByKey((prix, sum)-> sum + prix);

		// save to outputfile
		File resultFile = new File("ex1_"+outputFile+".csv");
		if(!resultFile.exists()) resultFile.createNewFile();
		//ventesByVille.coalesce(1).saveAsTextFile(outputFile);
		JavaRDD<String> ventesLines = ventesByVille.map((pair)->{
			return pair._1()+ ","+pair._2();
		});
		// to local file
		FileUtils.writeLines(resultFile,  fileHeaderRDD.union(ventesLines).collect());

		// to hadoop
		File resultTxtFile = new File("ex1_"+outputFile);
		if(resultTxtFile.exists()) FileUtils.deleteQuietly(resultTxtFile);
		ventesByVille.coalesce(1).saveAsTextFile(resultTxtFile.getName());
	}

	public static void runExo2(String inputFile,String outputFile, String year) throws IOException {
		/*
		  application permettant de calculer le prix total
		des ventes des produits par ville pour une année donnée.
		 */

		JavaSparkContext sparkContext = initSparkContext();

		// 1: get rdd from inputfile
		if(inputFile == null || !(new File(inputFile)).exists()) throw new RuntimeException("Input file invalid or does not exist");
		JavaRDD<String> ventesFileContent = sparkContext.textFile(inputFile);
		// remove file header
		JavaRDD<String> ventes = ventesFileContent.filter((venteLine)-> {
			return !venteLine.contains("ville");
		});



		//creating pair RDD<ville, prix>
		JavaPairRDD<String, Double> ventesPairs = ventes.mapToPair((venteline)->{
			Tuple2<String, Double> ventePrixTuple;
			String [] venteLineStrs = venteline.split(",");
			String anneVille = venteLineStrs[0]+"_"+venteLineStrs[1];
			Double prix = Double.valueOf(venteLineStrs[3]);
			ventePrixTuple = new Tuple2(anneVille,prix );
			return ventePrixTuple;
		});

		// reduce by the 'ville' and calculate the sum of ventes
		JavaPairRDD<String, Double> ventesByVille = ventesPairs.reduceByKey((prix, sum)-> sum + prix);

		// save to outputfile
		File resultFile = new File("ex2_"+outputFile+".csv");
		if(!resultFile.exists()) resultFile.createNewFile();

		JavaRDD<String> ventesLines = ventesByVille.map((pair)->{
			return pair._1()+ ","+pair._2();
		});

		// header
		JavaRDD<String> fileHeader = sparkContext.parallelize(Arrays.asList("annee_ville, total"));
		// to local file
		FileUtils.writeLines(resultFile, fileHeader.union(ventesLines).collect());

		// to hadoop
		File resultTxtFile = new File("ex2_"+outputFile);
		if(resultTxtFile.exists()) FileUtils.deleteQuietly(resultTxtFile);
		ventesByVille.coalesce(1).saveAsTextFile(resultTxtFile.getName());
	}
}
