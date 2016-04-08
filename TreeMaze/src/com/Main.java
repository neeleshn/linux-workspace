package com;

import java.util.ArrayList;

public class Main {
	
	public static float waitingTimeRobin(int[] arrival, int[] run, int q){
		
		float[] waitTime = new float[arrival.length];
		ArrayList<Integer> waiting = new ArrayList<>(); 
		
		for(int i =0 ; i<arrival.length;i++){
			waitTime[i]=0;
		}
		
		for(int i=0; i< arrival.length; i++){
			for(int j=0; j<arrival.length; j++){
				
			}
		}
		
		float tempSum=0;
		for(int i=0;i<arrival.length;i++){
			tempSum+=waitTime[i];
		}
		return tempSum/waitTime.length;
	}
	
	public static void main(String[] args){
		int [] arrival = {0,1,4};
		int [] run = {5,2,3};
		int q = 3;
		System.out.println(waitingTimeRobin(arrival, run,q));
	}
}
