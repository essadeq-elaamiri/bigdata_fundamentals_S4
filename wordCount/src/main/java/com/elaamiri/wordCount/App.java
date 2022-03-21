package com.elaamiri.wordCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * WordCount
 * @author essadeq
 * 
 */
public class App 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration config = new Configuration();
        // get files from arguments
        String[] files = new GenericOptionsParser(config,args).getRemainingArgs();
        Path inputPath = new Path(files[0]);
        Path outputPath =new Path(files[1]);
        
        // creating a job
        Job wordCountJob = new Job(config, "wordCountJob");
        
        // set job requirements
        wordCountJob.setJarByClass(App.class);
        wordCountJob.setMapperClass(MapperClass.class);
        wordCountJob.setReducerClass(ReducerClass.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        
        // set input, output paths
        FileInputFormat.addInputPath(wordCountJob, inputPath);
        FileOutputFormat.setOutputPath(wordCountJob, outputPath);
        
        // execute job
        System.exit(wordCountJob.waitForCompletion(true)?0:1);
        
        
        

        
        
        
    }
}
