package minMaxSalaire;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import minMaxSalaire.question1.MapperClass;
import minMaxSalaire.question1.ReducerClass;



public class App1 {
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
        // get files from arguments
        String[] files = new GenericOptionsParser(config,args).getRemainingArgs();
        Path inputPath = new Path(files[0]);
        Path outputPath =new Path(files[1]);
        
        System.out.println("In path: " + inputPath.toString());
        
        // creating a job
        Job minMaxSalaire = new Job(config, "minMaxSalaire");
        
        // set job requirements
        minMaxSalaire.setJarByClass(App1.class);
        minMaxSalaire.setMapperClass(MapperClass.class);
        minMaxSalaire.setReducerClass(ReducerClass.class);
        minMaxSalaire.setOutputKeyClass(Text.class);
        minMaxSalaire.setOutputValueClass(DoubleWritable.class);
        
        // set input, output paths
        FileInputFormat.addInputPath(minMaxSalaire, inputPath);
        FileOutputFormat.setOutputPath(minMaxSalaire, outputPath);
        
        // execute job
        System.exit(minMaxSalaire.waitForCompletion(true)?0:1);
	}
}
