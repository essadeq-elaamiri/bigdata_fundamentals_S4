

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



// returns the min and max Temp for every month in 1916
public class AppMinMaxTemp {
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
        // get files from arguments
        String[] files = new GenericOptionsParser(config,args).getRemainingArgs();
        Path inputPath = new Path(files[0]);
        Path outputPath =new Path(files[1]);
        
        // if output path exists recreate it
        File file = new File(outputPath.getName());
        if(file.exists() && file.isDirectory()) {
        	file.delete();
        }
        
        System.out.println("In path: " + inputPath.toString());
        
        // creating a job
        Job mainMaxTemp = new Job(config, "mainMaxTemp");
        
        // set job requirements
        mainMaxTemp.setJarByClass(AppMinMaxTemp.class);
        mainMaxTemp.setMapperClass(MapperClass.class);
        mainMaxTemp.setReducerClass(ReducerClass.class);
        
        mainMaxTemp.setOutputKeyClass(Text.class);
        mainMaxTemp.setOutputValueClass(DoubleWritable.class);
        
        // set input, output paths
        FileInputFormat.addInputPath(mainMaxTemp, inputPath);
        FileOutputFormat.setOutputPath(mainMaxTemp, outputPath);
        
        // execute job
        System.exit(mainMaxTemp.waitForCompletion(true)?0:1);
	}
}
