// Author: Habiba Neelesh
package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MeanReducer extends Reducer<Text,Text,Text,Text> {
	Text customTextVal = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
		int counter = 0;
		boolean active = false;
		String eachValue;
		String[] valuesArray;
		double[] priceSum = new double[12];
		int[] monthCounter = new int[12];

		for(int i=0; i<12; i++){
			priceSum[i] = 0.0;
			monthCounter[i] = 0;
		}
		
		for (Text val : values) {
			counter ++;
			eachValue= val.toString();
			valuesArray = eachValue.split("-");
			if(valuesArray[0].equals("2015")){
				active=true;
			}
			
			int month = Integer.parseInt(valuesArray[1])-1;
			double price = Double.parseDouble(valuesArray[2]);
			priceSum[month]+=price;
			monthCounter[month]++;
		}
		
		if(active){
			for(int i=0; i<12; i++){
				if(monthCounter[i]>0){
					double avgPrice = priceSum[i]/monthCounter[i];
					String customValue = avgPrice+"\t"+(i+1)+"\t"+counter;
					customTextVal.set(customValue);
					context.write(key,customTextVal);
				}
			}
		}
	}
}

