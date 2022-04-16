package com.elaamiri.kmeansPoint;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

public class KmeansUtils {

	private static FileHandler handler;
	private static Logger logger;
	private static boolean append = true;
	static{
		try {
			handler = new FileHandler("kmeansMapReduce.log", append);
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger = Logger.getLogger("kmeansMapReduce");
		logger.addHandler(handler);
	}

	public static void logToFile(String msg, boolean append2) throws SecurityException, IOException {
		append = append2;
		logger.info(msg);
	}



	// calculating mean
	public static Point calculateMean(List<Point> points) {
		double sumX = 0, sumY = 0;
		int numberOfPoints = points.size();
		if (points == null || points.isEmpty()) throw new RuntimeException("Points list is empty!");
		for(Point point: points) {
			sumX += point.getPointX();
			sumY += point.getPointY();
		}

		return new Point(sumX/numberOfPoints, sumY/numberOfPoints);
	}

	public static List<Point> getPointsListFromFile(File file) throws IOException{
		List<Point> points = new ArrayList<>();

		List<String> poinstListString = FileUtils.readLines(file);

		for(String poinstString: poinstListString) {
			points.add(new Point(poinstString.trim()));
		}

		return points;

	}

	public static boolean isTheSameCenters(List<Point> centers1, List<Point> centers2 ) {
		if(centers1.size() != centers2.size()) return false;
		int it = centers1.size();
		for(int i=0; i<it; i++) {
			if(!centers1.get(i).isEqualTo(centers2.get(i))) {
				return false;
			}
		}
		return true;
	}





}
