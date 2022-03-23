

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	protected void reduce(Text keyMonth, Iterable<DoubleWritable> tempList, Context context
            ) throws IOException, InterruptedException {
		double maxTemp = Double.MIN_VALUE;
		double minTemp = Double.MAX_VALUE;
		Iterator<DoubleWritable> tempIterator = tempList.iterator();
		// skip the first line, the header
		tempIterator.next();
		System.out.println("Reduce: "+ keyMonth);
		while(tempIterator.hasNext()) {
			Double currentTemp = tempIterator.next().get();
			if(currentTemp > maxTemp) maxTemp = currentTemp;
			if(currentTemp < minTemp) minTemp = currentTemp;
		}
		
		
		String minTempTextOut = keyMonth.toString().concat(" min ");
		String maxTempTextOut = keyMonth.toString().concat(" max ");
		
		System.out.println(maxTemp+ ", " + minTemp);

		//context.write(new Text(outDep), new Text(outSalaire));
		context.write(new Text(maxTempTextOut), new DoubleWritable(maxTemp));
		context.write(new Text(minTempTextOut), new DoubleWritable(minTemp));

		
	}
}
