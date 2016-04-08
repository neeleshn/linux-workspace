package neeluxx;

import java.util.*;

public class Main {
	
	public static void main(String[] args){
		String ranges = "";
		compactAndSort(ranges);
	}
	
	public static String compactAndSort(String ranges){
		//String[] rangesArray = (ranges.split(",")).toString().trim();
		String result="";
	       
	       if(ranges != null && !ranges.isEmpty()){        
		       String[] rangesArray = ranges.split(",");
		       Map<Integer,Integer> values = new HashMap<Integer,Integer>();
		       for(String eachRange: rangesArray){
		           eachRange = eachRange.trim();
		           String[] rangePart = eachRange.split(":");
		           int key = Integer.parseInt(rangePart[0].trim());
		           int value = Integer.parseInt(rangePart[1].trim());
		           if(values.containsKey(key)){
		               int oldValue = values.get(key);
		               if(oldValue < value)
		                   values.put(key,value);                
		           }else{
		               values.put(key,value);
		           }            
		       }
		       
		       TreeMap<Integer,Integer> sortedRanges = new TreeMap<Integer,Integer>(values);
		       Set<Integer> sortedKeySet = sortedRanges.keySet();
		       
		       int counter = 0;
		       int oldVal = 0;
		       
		       for(int newKey: sortedKeySet){
		           if(counter == 0){                
		               oldVal = sortedRanges.get(newKey);
		               counter++;
		               result = newKey+":";
		               continue;
		           }
		           if((oldVal+1) >= newKey){
		               oldVal = sortedRanges.get(newKey);
		           } else {
		               result += oldVal + "," + newKey + ":";
		               oldVal = sortedRanges.get(newKey);
		           }
		       }
		       
		       result += oldVal;       
	       }
	       
	       return result;
	}
}
