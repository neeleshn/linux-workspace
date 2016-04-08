import java.io.File;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

public class ThreadedAnalyser {

	public static int k=0,f=0;
	public static HashSet<String> topCarriers = new HashSet<String>();
	public static HashSet<String> activeCarriers = new HashSet<String>();
	public static HashMap<String, ArrayList<Float>> allPricesMap = new HashMap<String, ArrayList<Float>>();
	public static HashMap<String, ArrayList<Float>> monthCarrierMap = new HashMap<String, ArrayList<Float>>();
	public static String location;
	public static int toCalculate;
	
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
	
	public static synchronized void monthPopulate(String carrierId, float avgPrice){
		
		if(monthCarrierMap.containsKey(carrierId)){
			ArrayList<Float> a1 = monthCarrierMap.get(carrierId);
			a1.add(avgPrice);
			monthCarrierMap.put(carrierId, a1);
		} else{
			ArrayList<Float> a1 = new ArrayList<>();
			a1.add(avgPrice);
			monthCarrierMap.put(carrierId, a1);
		}
	}

	public static void main(String[] args) throws Exception{
		
		try{
			location=args[0];
			toCalculate = Integer.parseInt(args[1]);
		} catch (Exception e){
			location= "resources/all";
			toCalculate = 0;
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
				
		List<Map.Entry<String, ArrayList<Float>>> list = new LinkedList<Map.Entry<String, ArrayList<Float>>>(allPricesMap.entrySet());

		Collections.sort(list, new Comparator<Map.Entry<String, ArrayList<Float>>>() {
			public int compare(Map.Entry<String, ArrayList<Float>> o1, Map.Entry<String, ArrayList<Float>> o2) {
				if (o2.getValue().size() > o1.getValue().size()){
					return 1;
				} else {
					return -1;
				}
			}
		});

		int counter = 1;
		for(Map.Entry<String, ArrayList<Float>> entry:list){
			if(activeCarriers.contains(entry.getKey())){
				topCarriers.add(entry.getKey());
				if(counter==10){
					break;
				}
				counter++;
			}
		}

		Iterator<Entry<String,ArrayList<Float>>> itr = monthCarrierMap.entrySet().iterator();
		while(itr.hasNext()){
			Map.Entry<String, ArrayList<Float>> entry = (Map.Entry<String, ArrayList<Float>>)itr.next();
			String[] keyArray = entry.getKey().split("-");
			String carrierId = keyArray[0];
			String month = keyArray[1];
			if(topCarriers.contains(carrierId)){
				ArrayList<Float> allPrices = entry.getValue();
				int listSize = allPrices.size();
				if(toCalculate == 0){
					float sum = 0;
					for(float eachPrice : allPrices){
						sum+=eachPrice;
					}
					float meanPrice = sum/listSize;
					System.out.println(month+" \t "+carrierId+" \t "+meanPrice);

				} else if(toCalculate == 1){
					
					Collections.sort(allPrices);
					float medianPrice = 0;
					if(listSize%2==0){
						medianPrice = (allPrices.get(listSize/2) + allPrices.get((listSize-1)/2))/2;
					} else {
						medianPrice = allPrices.get((listSize-1)/2);
					}
					System.out.println(month+" \t "+carrierId+" \t "+medianPrice);
				} else {
					System.out.println("fast median");
					break;
				}
			}
		}
	}
}

