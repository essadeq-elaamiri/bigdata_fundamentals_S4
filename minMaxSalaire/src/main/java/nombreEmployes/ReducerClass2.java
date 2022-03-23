package nombreEmployes;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReducerClass2 
extends MapReduceBase 
implements Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(
			Text key, 
			Iterator<IntWritable> values, 
			OutputCollector<Text, IntWritable> output, 
			Reporter reporter)
			throws IOException {
		System.out.println("ello Reducer");
		
		int sum = 0;
		while(values.hasNext()) {
			sum ++;
			System.out.println(sum);
			values.next();
		}

		//context.write(new Text(outDep), new Text(outSalaire));
		output.collect(key, new IntWritable(sum));
		
	}
}