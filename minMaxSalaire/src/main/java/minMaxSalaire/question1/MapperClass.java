package minMaxSalaire.question1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperClass extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
		
		// nom,prenom,departement,metier,salaire
		String[] lineSplitted = line.toString().trim().split(",");
		String departementValueOut = lineSplitted[2]; // key
		double salaireValueOut = Double.valueOf(lineSplitted[4]); // value
		System.out.println(departementValueOut+ " "+ salaireValueOut);
		
		context.write(new Text(departementValueOut),  new DoubleWritable(salaireValueOut));
	}
}
