package nombreEmployes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapperClass2 
extends MapReduceBase 
implements Mapper<LongWritable, Text, Text, IntWritable>
{
	
	public void map(
			LongWritable key,
			Text line, 
			OutputCollector<Text, IntWritable> output, 
			Reporter reporter)
			throws IOException 
	{
		// nom,prenom,departement,metier,salaire
		System.out.println("hello Mapper");
				String[] lineSplitted = line.toString().trim().split(",");
				String departementKeyOut = lineSplitted[2]; // key
				String employeValueOut = lineSplitted[0]; // value
				
				output.collect(new Text(departementKeyOut),  new IntWritable(1));
		System.out.println("Hello 12");
	}
}
