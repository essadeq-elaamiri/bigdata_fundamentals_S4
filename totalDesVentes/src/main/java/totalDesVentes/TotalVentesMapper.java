package totalDesVentes;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TotalVentesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
		// line is like this : date ville produit prix
		// I will return ville as a key and prix as a value
		String[] lineValues = line.toString().toLowerCase().trim().split(" ");
		System.out.println("************ "+lineValues.length);
		String keyVille = lineValues[1];
		double valuePrix = Double.valueOf(lineValues[3]);
		
		context.write(new Text(keyVille),  new DoubleWritable(valuePrix));
	}

}
