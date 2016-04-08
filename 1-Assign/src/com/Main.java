package com;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Main {

	public static int k=0,f=0;
	public static HashMap<String, ArrayList<Float>> allPricesMap = new HashMap<String, ArrayList<Float>>();
	public static HashSet<String> activeCarriers = new HashSet<String>();
	public static String location;
	
	public static synchronized void kIncrement(){
		++k;
	}
	
	public static synchronized void fIncrement(){
		++f;
	}
	
	public static synchronized void addCarrier(String carrierId){
		activeCarriers.add(carrierId);
	}
	
	public static synchronized void mapPopulate(String carrierId, float avgPrice){
		
		if(allPricesMap.containsKey(carrierId)){
			ArrayList<Float> a1 = allPricesMap.get(carrierId);
			a1.add(avgPrice);
			allPricesMap.put(carrierId, a1);
			
		} else{
			ArrayList<Float> a1 = new ArrayList<>();
			a1.add(avgPrice);
			allPricesMap.put(carrierId, a1);
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		try{
			location=args[1];
		} catch (Exception e){
			location= "resources/all";
		}
		
		File inputDir = new File(location);
		File[] allCsvGz= inputDir.listFiles();
		
		int filesCount = allCsvGz.length;
		SanityThread[] extract = new SanityThread[filesCount];
		
		for(int i=0; i<filesCount; i++){
			String csvGzName = allCsvGz[i].getName();
			extract[i] = new SanityThread(csvGzName);
			extract[i].start();
		}
		
		for(int i=0; i<filesCount; i++){
			extract[i].join();
		}
		
		System.out.println("K: "+k);
		System.out.println("F: "+f);
		
		Iterator<Entry<String,ArrayList<Float>>> itr = allPricesMap.entrySet().iterator();
		while(itr.hasNext()){
			Map.Entry<String, ArrayList<Float>> entry = (Map.Entry<String, ArrayList<Float>>)itr.next();
			String carrierId = entry.getKey();
			if(activeCarriers.contains(carrierId)){
				ArrayList<Float> allPrices = entry.getValue();
				Collections.sort(allPrices);
				float sum = 0;
				for(float eachPrice : allPrices){
					sum+=eachPrice;
				}
				float avgPrice = sum/allPrices.size();
				float medianPrice = 0;
				int mapSize = allPrices.size();
				if(mapSize%2==0){
					medianPrice = (allPrices.get(mapSize/2) + allPrices.get((mapSize-1)/2))/2;
				} else {
					medianPrice = allPrices.get((mapSize-1)/2);
				}
				
				System.out.println(carrierId+"\t"+avgPrice+"\t"+medianPrice);
			}
		}
		
	}
}

