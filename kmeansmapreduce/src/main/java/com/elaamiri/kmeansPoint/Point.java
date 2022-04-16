package com.elaamiri.kmeansPoint;

public class Point  {
	private double pointX;
	private double pointY;
	public Point(double pointX, double pointY) {
		super();
		this.pointX = pointX;
		this.pointY = pointY;
	}
	
	public Point(String txt) {
		if(txt == null || txt.isEmpty() || !txt.contains(",")) throw new RuntimeException("Not valid point string");
		String [] coords = txt.split(",");
		this.pointX = Double.parseDouble(coords[0]);
		this.pointY = Double.parseDouble(coords[1]);

	}
	
	public double getPointX() {
		return pointX;
	}
	public void setPointX(double pointX) {
		this.pointX = pointX;
	}
	public double getPointY() {
		return pointY;
	}
	public void setPointY(double pointY) {
		this.pointY = pointY;
	}
	
	public double getDestanceFrom(Point point) {
		return Math.abs(Math.sqrt(Math.pow((this.pointX - point.getPointX()), 2) + Math.pow((this.pointY - point.getPointY()), 2)));
	}
	
	public String toString() {
		return this.pointX+ "," + this.pointY;
	}
	
	public String toStringJson() { // parenthesis
		return "{\"x\":"+this.pointX+ ", \"y\":" + this.pointY+"}";
	}
	
	public static Point getPointFromString(String pointStr) {
		// TODO: more validation
		if(pointStr == null || pointStr.isEmpty() || !pointStr.contains(",")) throw new RuntimeException("Not valid point string");
		String [] coords = pointStr.split(",");
		return new Point(Double.parseDouble(coords[0]) ,Double.parseDouble(coords[1]));
	}

	
	public boolean isEqualTo(Point p) {
		return ((this.pointX == p.getPointX()) && (this.pointY== p.getPointY()));
	}
}
