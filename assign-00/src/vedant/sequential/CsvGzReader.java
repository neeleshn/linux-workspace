package vedant.sequential;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import com.opencsv.CSVReader;

public class CsvGzReader {

	private ArrayList<String> csvHeader = new ArrayList<String>();
	private HashMap<String, ArrayList<Float>> ticketPriceMap = new HashMap<String, ArrayList<Float>>();
	private HashMap<String, Float> averagePriceMap = new HashMap<String, Float>();
	private ArrayList<Entry<String, Float>> outputPriceList = new ArrayList<Entry<String, Float>>();
	
	public void processGZipFile(String gzipFile) {
		long validCount = 0;
        long invalidCount = 0;
        
		try {
        	InputStream fileStream = new FileInputStream(gzipFile);
        	InputStream gzipStream = new GZIPInputStream(fileStream);
        	Reader decoder = new InputStreamReader(gzipStream, "ASCII");
        	CSVReader buffered = new CSVReader(decoder);

            String[] fields;
            
            csvHeader = new ArrayList<String>(Arrays.asList(buffered.readNext()));
            
            while((fields = buffered.readNext())!=null){                
            	if(isRecordValid(fields)){
            		validCount += 1;  		
            		processValidEntry(fields);
              	} else {
            		invalidCount += 1;
            	}
            }
            
            buffered.close();
            decoder.close();
            
        } catch (IOException e) {
        	System.err.println("UNABLE TO DECOMPRESS FILE:");
            e.printStackTrace();
        }
        
		// Compute average
		computeAverage();
		sortAverages();
		
        // Print output
        System.out.println(invalidCount);
        System.out.println(validCount);
        
        for (Entry<String, Float> e : outputPriceList){
        	System.out.println(e.getKey() + " " + e.getValue());
        }   
    }
	
	private void sortAverages() {
		outputPriceList = new ArrayList<Entry<String, Float>>(averagePriceMap.entrySet());
        Collections.sort(outputPriceList, new Comparator<Map.Entry<String, Float>>() {
            public int compare( Map.Entry<String, Float> o1, Map.Entry<String, Float> o2 ) {
                return -(o2.getValue()).compareTo( o1.getValue() );
            }
        }); 
	}

	private void computeAverage() {
		for(String carrier : ticketPriceMap.keySet()){
			ArrayList<Float> allPrices = ticketPriceMap.get(carrier);
			float avg = 0;
			for(float price : allPrices){ avg += price; }
			avg = avg / allPrices.size();
			averagePriceMap.put(carrier, avg);
		}
	}

	private void processValidEntry(String[] fields) {
		float ticketPrice = Float.parseFloat(fields[csvHeader.indexOf("AVG_TICKET_PRICE")]);
		String carrierId = fields[csvHeader.indexOf("CARRIER")];
		ArrayList<Float> allPrices = null;
		
		if(ticketPriceMap.containsKey(carrierId)){
			allPrices = ticketPriceMap.get(carrierId);
		} else {
			allPrices = new ArrayList<Float>();
		}

		allPrices.add(new Float(ticketPrice));
		ticketPriceMap.put(carrierId, allPrices);
	}

	private boolean isRecordValid(String[] fields) {
		
		float timeZone=0;
		
		// CRSArrTime and CRSDepTime should not be zero
		// timeZone % 60 should be 0
		
		SimpleDateFormat format = new SimpleDateFormat("HHmm");
		
		String CRS_ARR_TIME = fields[csvHeader.indexOf("CRS_ARR_TIME")];
		String CRS_DEP_TIME = fields[csvHeader.indexOf("CRS_DEP_TIME")];
		String CRS_ELAPSED_TIME = fields[csvHeader.indexOf("CRS_ELAPSED_TIME")];
	
		try{
			Date CRSArrTime = (CRS_ARR_TIME.equals("") ? null : format.parse(CRS_ARR_TIME));
			Date CRSDepTime = (CRS_DEP_TIME.equals("") ? null : format.parse(CRS_DEP_TIME));
			float CRSElapsedTime = Float.parseFloat(CRS_ELAPSED_TIME);
			
			float crsDiff = hhmmDiff(CRS_ARR_TIME, CRS_DEP_TIME);
			
			timeZone = crsDiff - CRSElapsedTime;
			
			if (CRSArrTime.getTime() == 0.0 || CRSDepTime.getTime() == 0.0) return false;
			if ((timeZone % 60) != 0) return false;
		
		} catch(NumberFormatException e) {
			return false;
		} catch (ParseException e) {
			return false;
		}
		
		// AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
		
		int ORIGIN_AIRPORT_ID = Integer.parseInt(fields[csvHeader.indexOf("ORIGIN_AIRPORT_ID")]);
		if (ORIGIN_AIRPORT_ID <= 0) return false;
		
		int ORIGIN_AIRPORT_SEQ_ID = Integer.parseInt(fields[csvHeader.indexOf("ORIGIN_AIRPORT_SEQ_ID")]);
		if (ORIGIN_AIRPORT_SEQ_ID <= 0) return false;
		
		int ORIGIN_CITY_MARKET_ID = Integer.parseInt(fields[csvHeader.indexOf("ORIGIN_CITY_MARKET_ID")]);
		if (ORIGIN_CITY_MARKET_ID <= 0) return false;
		
		int ORIGIN_STATE_FIPS = Integer.parseInt(fields[csvHeader.indexOf("ORIGIN_STATE_FIPS")]);
		if (ORIGIN_STATE_FIPS <= 0) return false;
		
		int ORIGIN_WAC = Integer.parseInt(fields[csvHeader.indexOf("ORIGIN_WAC")]);
		if (ORIGIN_WAC <= 0) return false;
		
		int DEST_AIRPORT_ID = Integer.parseInt(fields[csvHeader.indexOf("DEST_AIRPORT_ID")]);
		if (DEST_AIRPORT_ID <= 0) return false;
		
		int DEST_AIRPORT_SEQ_ID = Integer.parseInt(fields[csvHeader.indexOf("DEST_AIRPORT_SEQ_ID")]);
		if (DEST_AIRPORT_SEQ_ID <= 0) return false;
		
		int DEST_CITY_MARKET_ID = Integer.parseInt(fields[csvHeader.indexOf("DEST_CITY_MARKET_ID")]);
		if (DEST_CITY_MARKET_ID <= 0) return false;
		
		int DEST_STATE_FIPS = Integer.parseInt(fields[csvHeader.indexOf("DEST_STATE_FIPS")]);
		if (DEST_STATE_FIPS <= 0) return false;
		
		int DEST_WAC = Integer.parseInt(fields[csvHeader.indexOf("DEST_WAC")]);
		if (DEST_WAC <= 0) return false;
		
		// Origin, Destination,  CityName, State, StateName should not be empty

		String ORIGIN = fields[csvHeader.indexOf("ORIGIN")];
		if (ORIGIN.equals("")) return false;
		
		String ORIGIN_CITY_NAME = fields[csvHeader.indexOf("ORIGIN_CITY_NAME")];
		if (ORIGIN_CITY_NAME.equals("")) return false;
		
		String ORIGIN_STATE_ABR = fields[csvHeader.indexOf("ORIGIN_STATE_ABR")];
		if (ORIGIN_STATE_ABR.equals("")) return false;
		
		String ORIGIN_STATE_NM = fields[csvHeader.indexOf("ORIGIN_STATE_NM")];
		if (ORIGIN_STATE_NM.equals("")) return false;
		
		String DEST = fields[csvHeader.indexOf("DEST")];		
		if (DEST.equals("")) return false;
		
		String DEST_CITY_NAME = fields[csvHeader.indexOf("DEST_CITY_NAME")];
		if (DEST_CITY_NAME.equals("")) return false;
		
		String DEST_STATE_ABR = fields[csvHeader.indexOf("DEST_STATE_ABR")];
		if (DEST_STATE_ABR.equals("")) return false;
		
		String DEST_STATE_NM = fields[csvHeader.indexOf("DEST_STATE_NM")];
		if (DEST_STATE_NM.equals("")) return false;
		
		
		// For flights that are not Cancelled:
	
		int CANCELLED = Integer.parseInt(fields[csvHeader.indexOf("CANCELLED")]);		
		
		if (CANCELLED != 1){			
			
			String ARR_TIME = fields[csvHeader.indexOf("ARR_TIME")];
			String DEP_TIME = fields[csvHeader.indexOf("DEP_TIME")];
			String ACTUAL_ELAPSED_TIME = fields[csvHeader.indexOf("ACTUAL_ELAPSED_TIME")];
			
			try{
				long actualElapsedTime = Long.parseLong(ACTUAL_ELAPSED_TIME);
				long actualDiff = hhmmDiff(ARR_TIME, DEP_TIME);
				
				long actualTimeZone = actualDiff - actualElapsedTime;

				long CRSElapsedTime = Long.parseLong(CRS_ELAPSED_TIME);
				long crsDiff = hhmmDiff(CRS_ARR_TIME, CRS_DEP_TIME);
				long newtimeZone = crsDiff - CRSElapsedTime;

				if (actualTimeZone != newtimeZone) {
					return false;
				}
				
				// if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
				// if ArrDelay < 0 then ArrDelayMinutes should be zero
				// if ArrDelayMinutes >= 15 then ArrDel15 should be false
				
				float arrDelay = Float.parseFloat(fields[csvHeader.indexOf("ARR_DELAY")]);
				
				float ArrDelayMinutes = Float.parseFloat(fields[csvHeader.indexOf("ARR_DELAY_NEW")]);
				
				float arrDel15 = Float.parseFloat(fields[csvHeader.indexOf("ARR_DEL15")]);
				
				if (arrDelay > 0.0){
					if (arrDelay != ArrDelayMinutes) {
						return false;
					}
				}

				if (arrDelay < 0.0){
					if (ArrDelayMinutes != 0) {
						return false;
					}
				}
				
				if (ArrDelayMinutes > 15.0){
					if (arrDel15 != 1) {
						return false;
					}
				}
								
			} catch(NumberFormatException e) {
				// no entry found in record
				// format does not match
				return false;
			}
		}
		// All validations passed
		return true;
	}

	private int hhmmDiff (String arr, String dep){
		if (Integer.parseInt(arr) > Integer.parseInt(dep)){
			return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2))) * 60 +
					(Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
		} else {
			// Cross over 24hr
			return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2)) + 24) * 60 +
					(Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
		}
	}
	
}
