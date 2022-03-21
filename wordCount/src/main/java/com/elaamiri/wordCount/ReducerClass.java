package com.elaamiri.wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
	// input <word, value>
	// output <word, value>
	public void reduce(Text inputWord, Iterable<IntWritable> inputValues, Context context) throws IOException, InterruptedException {
		// an intermediate process have combined the value of the same word in a set
		// (word1, (1,1)), (word2, (1,1, 1))
		
		int sum = 0;
		for(IntWritable val: inputValues) {
			sum += val.get();
		}
		context.write(inputWord, new IntWritable(sum));
		
	}
}
