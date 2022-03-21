package totalDesVentesAnneeDonnee;

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



public class App2 {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
        // get files from arguments
        String[] files = new GenericOptionsParser(config,args).getRemainingArgs();
        Path inputPath = new Path(files[0]);
        Path outputPath =new Path(files[1]);
        
        // creating a job
        Job totalDesVentesAnneeDonnee = new Job(config, "totalDesVentesAnneeDonnee");
        
        // set job requirements
        totalDesVentesAnneeDonnee.setJarByClass(App2.class);
        totalDesVentesAnneeDonnee.setMapperClass(TotalVentesMapper.class);
        totalDesVentesAnneeDonnee.setReducerClass(TotalVentesReducer.class);
        totalDesVentesAnneeDonnee.setOutputKeyClass(Text.class);
        totalDesVentesAnneeDonnee.setOutputValueClass(DoubleWritable.class);
        
        // set input, output paths
        FileInputFormat.addInputPath(totalDesVentesAnneeDonnee, inputPath);
        FileOutputFormat.setOutputPath(totalDesVentesAnneeDonnee, outputPath);
        
        // execute job
        System.exit(totalDesVentesAnneeDonnee.waitForCompletion(true)?0:1);
	}

}
