package kMeans;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.sound.midi.Soundbank;

/**
 * 
 * @author essadeq elaamiri
 *	Implementation of K-means clusters algorithm
 * @date 2022-03-28
 */
public class mainApp {
	private static final int numberOfClusters = 3;
	private static final int numberOfPoints = 100;
	

	
	public static void main(String[] args) {
		List<Double> points = new ArrayList<Double>();
		List<Cluster> clusters = new ArrayList<Cluster>(); 
		// init points
		for(int i=0; i<numberOfPoints; i++) {
			points.add(Math.random()*1000d);
		}
		
		// init cluster's centers
		for(int i=0; i<numberOfClusters; i++) {
			clusters.add(new Cluster(Math.random()* 100d));
			System.out.println(clusters.size());
		}
		
		double destance1, destance2, destance3;
		// 1st iteration
		for(double point : points ) {
			destance1 = Math.abs(point - clusters.get(0).getCenter());
			destance2 = Math.abs(point - clusters.get(1).getCenter());
			destance3 = Math.abs(point - clusters.get(2).getCenter());
			double minimum = Math.min(destance1, Math.min(destance2, destance3));
			
			Optional<Cluster> op;
			if(destance1 == minimum) {
				op = Optional.of(clusters.get(0));
				if(op.isPresent()) {
					System.out.println(op.get().getPoints() == null);
				}
			}
			else if(destance2 == minimum) {
				op = Optional.of(clusters.get(1));
				if(op.isPresent()) {
					//op.get().getPoints().add(point);
				}
			}
			else if(destance3 == minimum) {
				op = Optional.of(clusters.get(2));
				if(op.isPresent()) {
					//op.get().getPoints().add(point);
				}
			}
			
			
		}
		
		System.out.println(clusters.get(0));
		System.out.println(clusters.get(1));
		System.out.println(clusters.get(2));

		
	}
}
