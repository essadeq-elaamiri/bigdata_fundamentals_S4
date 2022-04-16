package com.elaamiri.kmeansPoint;
/// ma.enset.kmeansmapreduce.KmeansDriver

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KmeansDriver {

	public static void main(String[] args) throws Exception {
		/**
		 * TODO:
		 * [ ]- iterate the operation while centers change
		 * [X]-or reach X iterations
		 * [X]-every time override the centers file with the new centers
		 * [ ]- Execute it on Hadoop
		 * [ ]- Visualise it using Java/ JavaScript/ Pyhton
		 * 
		 */



		int iterationsNumber = 0;
		int numberOfTimeTheSameCentersReached = 0;
		while (iterationsNumber < 10 && numberOfTimeTheSameCentersReached < 3) {
			numberOfTimeTheSameCentersReached += runJob(iterationsNumber, "data/pointsData.csv", null);
			System.out.println(numberOfTimeTheSameCentersReached +" convergences.");
			KmeansUtils.logToFile(numberOfTimeTheSameCentersReached +" convergences.", true);
			iterationsNumber ++;
		}



	}

	private static int runJob(int number, String inputPathStr, String outputPathStr) throws IllegalArgumentException, IOException, Exception {
		System.out.println("[JOB N°"+number+"]=================================");
		int convergence = 0 ; // the same centers
		// using data/centers.csv as outpout
		String outputPathString = "data/centers";
		String outputPathStrNumberd = outputPathString+"_";
		File outputPathFile = new File(outputPathStrNumberd);
		File centersFile = new File(outputPathString);
		File partR00000 = new File(outputPathStrNumberd+"/part-r-00000");
		List<Point> centers1 = new ArrayList<Point>(), centers2 = new ArrayList<Point>();
		if(outputPathFile.exists()) {
			// get all centers
			centers1 = KmeansUtils.getPointsListFromFile(centersFile);
			// get new centers
			centers2 = KmeansUtils.getPointsListFromFile(partR00000);
			if(KmeansUtils.isTheSameCenters(centers1, centers2)) {
				convergence = 1;
			}
			// overriding centers file
			FileUtils.copyFile(partR00000, centersFile);
			//outputPathFile.delete();
			FileUtils.deleteDirectory(outputPathFile);
		}




		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"Job Kmeans "+ number);
		job.setJarByClass(KmeansDriver.class);
		job.setMapperClass(KmeansMapper.class);
		job.setReducerClass(KmeansReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// read data file to memory
		//job.addCacheFile(new URI("hdfs://localhost:9000/data/centers.csv"));
		job.addCacheFile(new URI(outputPathString));

		FileInputFormat.addInputPath(job,new Path(inputPathStr));
		FileOutputFormat.setOutputPath(job,new Path(outputPathStrNumberd));

		job.waitForCompletion(true);

		return convergence;

	}

}
