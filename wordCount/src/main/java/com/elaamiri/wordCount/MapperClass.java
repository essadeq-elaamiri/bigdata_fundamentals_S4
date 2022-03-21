package com.elaamiri.wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	// <LongWritable (input key), Text (Input value),
	//Text (Output key), IntWritable (Output value)>
	public void map(LongWritable key, Text lineValue, Context context) throws IOException, InterruptedException {
		// each line of the data will be our input <key, lineValue>
		String line = lineValue.toString();
		String[] words = line.split(" "); // Splitting by space char
		
		// generate the map out put <word, 1>	
		
		for(String word: words) {
			Text outputKey = new Text(word.toUpperCase().trim());
			IntWritable outputValue = new IntWritable(1);
			// write that to the context
			context.write(outputKey, outputValue);
		}
	}

}
