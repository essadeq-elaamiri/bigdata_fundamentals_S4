package minMaxSalaire.question1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	protected void reduce(Text keyDepart, Iterable<DoubleWritable> salairesList, Context context
            ) throws IOException, InterruptedException {
		double maxSalaire = Double.MIN_VALUE;
		double minSalaire = Double.MAX_VALUE;
		for(DoubleWritable salaire: salairesList) {
			if(salaire.get() > maxSalaire) maxSalaire = salaire.get();
			if(salaire.get() < minSalaire) minSalaire = salaire.get();

		}
		String minDepOut = keyDepart.toString().concat(" min ");
		String maxDepOut = keyDepart.toString().concat(" max ");

		//context.write(new Text(outDep), new Text(outSalaire));
		context.write(new Text(maxDepOut), new DoubleWritable(maxSalaire));
		context.write(new Text(minDepOut), new DoubleWritable(minSalaire));

		
	}
}
