package com.elaamiri.kmeansPoint;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends Reducer<Text,Text,Text,Text> {
	private File file = null, resultDirectory =null;
    @Override
    protected void reduce(Text nearestCenter, Iterable<Text> points, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    	List<Point> pointsList = new ArrayList<>();
    	
    	System.out.println("\n ==============> Cluster center: "+ nearestCenter.toString());
    	int numeberOfpoints = 0;
    	
    	for(Text pointTxt : points ) {
    		pointsList.add(new Point(pointTxt.toString()));
    		System.out.print(pointTxt.toString()+ ", ");
    		numeberOfpoints ++;
    	}
    	
    	System.out.println("\n[Number of points : "+numeberOfpoints+"]");
    	
    	// write cluster point to a local file
    	resultDirectory = new File("resultData");
    	if(! resultDirectory.exists()) {
    		//file.delete();
    		resultDirectory.createNewFile();
    	}
    	file = new File("resultData/resData.json");
    	if(! file.exists()) {
    		//file.delete();
    		file.createNewFile();
    	}
    	
    	String resultPoints = pointsList.stream()
    		      .map(point -> point.toStringJson())
    		      .collect(Collectors.joining(",", "[", "]"));
    	
        FileUtils.write(file, "\n{\"clusterCenter\":"+new Point(nearestCenter.toString()).toStringJson()+", \"numberOfPoints\":"+numeberOfpoints+" ,\"points\":"+resultPoints+"}", true);
    	Point newCenter = KmeansUtils.calculateMean(pointsList);
        context.write(null,new Text(newCenter.toString()));
    }
}
