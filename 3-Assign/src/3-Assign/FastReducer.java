package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class FastReducer extends Reducer<Text,Text,Text,Text> {
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
					float medianPrice = fastMedian(allPrices.get(i), allPrices.get(i).size()/2);
					String customValue = medianPrice+"\t"+(i+1)+"\t"+counter;
					customTextVal.set(customValue);
					context.write(key,customTextVal);
				}
			}
		}
	}

	public static float fastMedian(ArrayList<Float> a, int k) {
		if(a.size()==0){
			return 0;
		}
	
		if (a.size()==1){
			return a.get(0);
		}
		int from = 0, to = a.size() - 1;
		// if from == to we reached the kth element
		try {
			while (from < to) {
				int r = from, w = to;
				float mid = a.get((r + w) / 2);
				// stop if the reader and writer meets
				while (r < w) {
					if (a.get(r) >= mid) { // put the large values at the end
						float tmp = a.get(w);
						a.set(w, a.get(r));
						a.set(r, tmp);
						w--;
					} else { // the value is smaller than the pivot, skip
						r++;
					}
				}
				// if we stepped up (r++) we need to step one down
				if (a.get(r) > mid)
					r--;
				// the r pointer is on the end of the first k elements
				if (k <= r) {
					to = r;
				} else {
					from = r + 1;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return 0;
		}
		return a.get(k);
	}

}



