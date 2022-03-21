package totalDesVentes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class App {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
        // get files from arguments
        String[] files = new GenericOptionsParser(config,args).getRemainingArgs();
        Path inputPath = new Path(files[0]);
        Path outputPath =new Path(files[1]);
        
        // creating a job
        Job prixTotalDesVentes = new Job(config, "prixTotalDesVentes");
        
        // set job requirements
        prixTotalDesVentes.setJarByClass(App.class);
        prixTotalDesVentes.setMapperClass(TotalVentesMapper.class);
        prixTotalDesVentes.setReducerClass(TotalVentesReducer.class);
        prixTotalDesVentes.setOutputKeyClass(Text.class);
        prixTotalDesVentes.setOutputValueClass(DoubleWritable.class);
        
        // set input, output paths
        FileInputFormat.addInputPath(prixTotalDesVentes, inputPath);
        FileOutputFormat.setOutputPath(prixTotalDesVentes, outputPath);
        
        // execute job
        System.exit(prixTotalDesVentes.waitForCompletion(true)?0:1);
	}

}
