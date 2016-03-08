package com.helpers;

public class Stopwatch {
	long start;
	public Stopwatch() {
		start = System.currentTimeMillis();
	}
	
	public double elapsedTime() {
		long now = System.currentTimeMillis();
		return (now - start)/1000.0;
	}
	
	public double elapsedTimeInMillis() {
		long now = System.currentTimeMillis();
		return (now - start);
	}
	
	public void printElapsedtime() {
		System.out.println("E T: " + elapsedTime());
	}
	
	public void printElapsedtimeAndReset() {
		System.out.println("E T: " + elapsedTime() + " Secs");
		reset();
	}
	
	public void printElapsedtimeAndReset(String msg) {
		System.out.println("E T ("+msg+"): " + elapsedTime() + " Secs");
		reset();
	}
		
	public void printElapsedtimeInMillisAndReset() {
		System.out.println("E T: " + elapsedTimeInMillis() + " Millis");
		reset();
	}
	
	public void printElapsedtimeInMillisAndReset(String msg) {
		System.out.println("E T ("+msg+"): " + elapsedTimeInMillis() + " Millis");
		reset();
	}

	public void printElapsedtimeInMillis(String msg) {
		System.out.println("E T ("+msg+"): " + elapsedTimeInMillis() + " Millis");
	}

	public void reset() {
		start = System.currentTimeMillis();
	}
	
}
