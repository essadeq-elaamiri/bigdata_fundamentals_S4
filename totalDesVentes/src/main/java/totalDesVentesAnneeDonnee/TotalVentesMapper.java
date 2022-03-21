package totalDesVentesAnneeDonnee;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalVentesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
		// line is like this : date ville produit prix
		// I will return ville as a key and prix as a value
		
		String[] lineValues = line.toString().toLowerCase().trim().split(" ");
		// get date 
		String dateIn = lineValues[0];
		String year = dateIn.split("/")[0];
		// contact year with the city name
		String keyVilleYear = lineValues[1].concat("_").concat(year);
		double valuePrix = Double.valueOf(lineValues[3]);
		
		context.write(new Text(keyVilleYear),  new DoubleWritable(valuePrix));
	}

}
