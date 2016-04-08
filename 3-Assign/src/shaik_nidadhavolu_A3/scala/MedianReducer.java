// Author: Habiba Neelesh
package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MedianReducer extends Reducer<Text,Text,Text,Text> {
	Text customTextVal = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
		int counter = 0;
		boolean active = false;
		String eachValue;
		String[] valuesArray;
		ArrayList<ArrayList<Float>> allPrices = new ArrayList<ArrayList<Float>>();
		ArrayList<Integer> monthCounter = new ArrayList<Integer>();

		for(int i=0; i<12; i++){
			allPrices.add(new ArrayList<Float>());
			monthCounter.add(0);
		}
		
		for (Text val : values) {
			counter ++;
			eachValue= val.toString();
			valuesArray = eachValue.split("-");
			if(valuesArray[0].equals("2015")){
				active=true;
			}
			
			int month = Integer.parseInt(valuesArray[1])-1;
			Float price = Float.parseFloat(valuesArray[2]);
			ArrayList<Float> tempPriceList = allPrices.get(month);
			tempPriceList.add(price);
			allPrices.set(month,tempPriceList);
			int tempCount = monthCounter.get(month);
			monthCounter.set(month,++tempCount);
		}
		
		if(active){
			for(int i=0; i<12; i++){
				if(monthCounter.get(i)>0){
					float medianPrice = 0;
					ArrayList<Float> tempPriceList = allPrices.get(i);
					Collections.sort(tempPriceList);
					if(monthCounter.get(i)%2 == 0){
						medianPrice= (tempPriceList.get(monthCounter.get(i)/2) + tempPriceList.get((monthCounter.get(i)-1)/2))/2;
					} else {
						medianPrice = tempPriceList.get((monthCounter.get(i)-1)/2);
					}
					String customValue = medianPrice+"\t"+(i+1)+"\t"+counter;
					customTextVal.set(customValue);
					context.write(key,customTextVal);
				}
			}
		}
	}
}

