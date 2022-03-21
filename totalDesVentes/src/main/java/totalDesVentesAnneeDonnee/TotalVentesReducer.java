package totalDesVentesAnneeDonnee;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TotalVentesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	protected void reduce(Text keyVille, Iterable<DoubleWritable> prixList, Context context
            ) throws IOException, InterruptedException {
		double totalDesVentes = 0;
		for(DoubleWritable prix: prixList) {
			totalDesVentes += prix.get();
		}
		context.write(keyVille, new DoubleWritable(totalDesVentes));
	}
}
