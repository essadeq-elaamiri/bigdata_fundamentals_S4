package kMeans;

import java.util.List;

public class Cluster {
	private double center;
	private List<Double> points;
	
	
	public Cluster(double center) {
		super();
		this.center = center;
	}
	public double getCenter() {
		return center;
	}
	public void setCenter(double center) {
		this.center = center;
	}
	public List<Double> getPoints() {
		return points;
	}
	public void setPoints(List<Double> points) {
		this.points = points;
	}
	@Override
	public String toString() {
		String desc = "{Center : "+ center + ", points :[";
		for(double p: points) {
			desc += p+", ";
		}
		desc += "]";
		return desc;
	}
	
	
}
