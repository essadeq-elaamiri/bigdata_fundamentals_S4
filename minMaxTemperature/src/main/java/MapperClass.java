

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperClass extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
		
		/*
		 * "STATION","DATE","SOURCE","LATITUDE","LONGITUDE",
		 * "ELEVATION","NAME","REPORT_TYPE","CALL_SIGN","QUALITY_CONTROL",
		 * "WND","CIG","VIS","TMP","DEW","SLP","GF1","KA1","EQD"
		 * line 1 to be skipped
		 */ 
		System.out.println("Map: "+ line.toString().subSequence(0, 10));
		
		// skip the first line 
		if(key.get() == 0 && line.toString().contains("STATION")) return;
		
		String[] lineSplitted = line.toString().trim().split("\",\"");
		String date = lineSplitted[1]; // key
		String temperatureFromLine = lineSplitted[13];
		String monthKeyOut = date.trim().split("-")[0].concat("-"+date.trim().split("-")[1]);
		System.out.println(monthKeyOut+", "+ lineSplitted[13]);
		String temperature = temperatureFromLine.replace(",", ".");
		//if(temperature.contains("+")) temperature = temperature.substring(temperature.indexOf("+"));
		double tempValueOut = Double.valueOf(temperature); 
		// value - double qoutes
		
		System.out.println(monthKeyOut+", "+ tempValueOut);
		
		context.write(new Text(monthKeyOut),  new DoubleWritable(tempValueOut));
	}
}
