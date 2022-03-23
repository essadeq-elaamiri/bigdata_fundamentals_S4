package minMaxSalaire;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;



import nombreEmployes.MapperClass2;
import nombreEmployes.ReducerClass2;

public class App2 {
	public static void main(String[] args) throws Exception {
        // get files from arguments
        Path inputPath = new Path(args[0]);
        Path outputPath =new Path(args[1]);
                
        // creating a job
        //Job nombrEmployes = new Job(config, "nombrEmployes");
        JobConf nombrEmployes = new JobConf();
        nombrEmployes.setJobName("nombrEmployes");
        
        // set job requirements
        nombrEmployes.setJarByClass(App2.class);
        nombrEmployes.setMapperClass(MapperClass2.class);
        nombrEmployes.setReducerClass(ReducerClass2.class);
        
        nombrEmployes.setOutputKeyClass(Text.class);
        nombrEmployes.setOutputValueClass(IntWritable.class);
        
        // set input, output paths
        FileInputFormat.addInputPath(nombrEmployes, inputPath);
        FileOutputFormat.setOutputPath(nombrEmployes, outputPath);
        
        // execute job
        //System.exit(nombrEmployes.waitForCompletion(true)?0:1);
        JobClient.runJob(nombrEmployes);
	}
}

