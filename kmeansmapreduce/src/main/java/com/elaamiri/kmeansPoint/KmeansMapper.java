package com.elaamiri.kmeansPoint;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansMapper extends Mapper<LongWritable, Text, Text,Text> {
    List<Point> centers=new ArrayList<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        centers.clear();
        URI uri[]= context.getCacheFiles();
        FileSystem fs=FileSystem.get(context.getConfiguration());
        InputStreamReader is=new InputStreamReader(fs.open(new Path(uri[0])));
        BufferedReader br=new BufferedReader(is);
        String line=null;
        String [] centersCoords = null;
        
        // reading centers file
        while ((line=br.readLine())!=null){
        	//centersCoords = line.split(" ");
        	/*
        	centers.add(
        			new Point(
        					Double.parseDouble(centersCoords[0]),
        					Double.parseDouble(centersCoords[1])));
        	*/
        	centers.add(Point.getPointFromString(line));
        }
    }

    @Override
    protected void map(LongWritable key, Text pointDataLine, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
      
    	//String [] pointCoords = pointDataLine.toString().split(" ");
        //double pointX=Double.parseDouble(pointCoords[0]);
        //double  pointY= Double.parseDouble(pointCoords[1]);
        
        Point point = Point.getPointFromString(pointDataLine.toString());
        
        double min=Double.MAX_VALUE,distance=0;
        Point nearestCenter = null;
        
        for (Point center:centers) {
            distance = center.getDestanceFrom(point);
            if (distance<min){
                min=distance;
                nearestCenter = center;
            }
        }
        
        context.write(new Text(nearestCenter.toString()),new Text(point.toString()));
    }

}
