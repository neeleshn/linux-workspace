package com;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.*;

public class SanityThread extends Thread{
	String csvGzName;
	public static ArrayList<String> csvHeader;
	
	public SanityThread(String name) {
		csvGzName = name;
	}
	
	
	private static boolean validRecord(CSVRecord row){
		try{
			// CRSArrTime and CRSDepTime should not be zero
			// timeZone % 60 should be 0

			String crsArrTime = row.get("CRS_ARR_TIME");
			String crsDepTime = row.get("CRS_DEP_TIME");
			int crsElapsedTime = Integer.parseInt(row.get("CRS_ELAPSED_TIME"));

			if (crsArrTime.equals("") || crsDepTime.equals("") || crsArrTime.equals("0") || crsDepTime.equals("0")){
				return false;
			}
			
			int crsDiff = hhmmDiff(crsArrTime, crsDepTime);
			
			int timeZone = crsDiff - crsElapsedTime;
			
			if ((timeZone % 60) != 0){
				return false;
			}
			
			
			// AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
			
			int originAirportId = Integer.parseInt(row.get("ORIGIN_AIRPORT_ID"));
			if (originAirportId <= 0) return false;
			
			int originAirportSeqId = Integer.parseInt(row.get("ORIGIN_AIRPORT_SEQ_ID"));
			if (originAirportSeqId <= 0) return false;
			
			int originCityMarketID = Integer.parseInt(row.get("ORIGIN_CITY_MARKET_ID"));
			if (originCityMarketID <= 0) return false;
			
			int originStateFips = Integer.parseInt(row.get("ORIGIN_STATE_FIPS"));
			if (originStateFips <= 0) return false;
			
			int originWac = Integer.parseInt(row.get("ORIGIN_WAC"));
			if (originWac <= 0) return false;
			
			int destAirportId = Integer.parseInt(row.get("DEST_AIRPORT_ID"));
			if (destAirportId <= 0) return false;
			
			int destAirportSeqId = Integer.parseInt(row.get("DEST_AIRPORT_SEQ_ID"));
			if (destAirportSeqId <= 0) return false;
			
			int destCityMarketId = Integer.parseInt(row.get("DEST_CITY_MARKET_ID"));
			if (destCityMarketId <= 0) return false;
			
			int destStateFips = Integer.parseInt(row.get("DEST_STATE_FIPS"));
			if (destStateFips <= 0) return false;
			
			int destWac = Integer.parseInt(row.get("DEST_WAC"));
			if (destWac <= 0) return false;
			
			
			// Origin, Destination,  CityName, State, StateName should not be empty

			String origin = row.get("ORIGIN");
			if (origin.equals("")) return false;
			
			String originCityName = row.get("ORIGIN_CITY_NAME");
			if (originCityName.equals("")) return false;
			
			String originStateAbr = row.get("ORIGIN_STATE_ABR");
			if (originStateAbr.equals("")) return false;
			
			String originStateName = row.get("ORIGIN_STATE_NM");
			if (originStateName.equals("")) return false;
			
			String dest = row.get("DEST");		
			if (dest.equals("")) return false;
			
			String destCityName = row.get("DEST_CITY_NAME");
			if (destCityName.equals("")) return false;
			
			String destStateAbr = row.get("DEST_STATE_ABR");
			if (destStateAbr.equals("")) return false;
			
			String destStateName = row.get("DEST_STATE_NM");
			if (destStateName.equals("")) return false;
			
			
			// For flights that are not Cancelled:
			
			int cancelled = Integer.parseInt(row.get("CANCELLED"));		
			
			if (cancelled != 1){			
				
				String arrTime = row.get("ARR_TIME");
				String depTime = row.get("DEP_TIME");
				int actualElapsedTime = Integer.parseInt(row.get("ACTUAL_ELAPSED_TIME"));
				
				int actualDiff = hhmmDiff(arrTime, depTime);
				
				int actualTimeZone = actualDiff - actualElapsedTime;
				
				crsDiff = hhmmDiff(crsArrTime, crsDepTime);
				int newtimeZone = crsDiff - crsElapsedTime;

				if (actualTimeZone != newtimeZone) {
					return false;
				}
				
				
				// if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
				// if ArrDelay < 0 then ArrDelayMinutes should be zero
				// if ArrDelayMinutes >= 15 then ArrDel15 should be false
				
				float arrDelay = Float.parseFloat(row.get("ARR_DELAY"));
				float arrDelayMinutes = Float.parseFloat(row.get("ARR_DELAY_NEW"));
				float arrDel15 = Float.parseFloat(row.get("ARR_DEL15"));
				
				if (arrDelay > 0.0){
					if (arrDelay != arrDelayMinutes) {
						return false;
					}
				}

				if (arrDelay < 0.0){
					if (arrDelayMinutes != 0) {
						return false;
					}
				}
				
				if (arrDelayMinutes > 15.0){
					if (arrDel15 != 1) {
						return false;
					}
				}
			}
		} catch(Exception e) {
			return false;
		}
		return true;
	}
	
	private static int hhmmDiff (String arr, String dep){
		if (Integer.parseInt(arr) > Integer.parseInt(dep)){
			return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2))) * 60 +
					(Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
		} else {
			return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2)) + 24) * 60 +
					(Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
		}
	}
	
	private static void populateOutputMap(CSVRecord row){
		String carrierId = row.get("UNIQUE_CARRIER");
		float avgPrice = Float.parseFloat(row.get("AVG_TICKET_PRICE"));
		Main.mapPopulate(carrierId, avgPrice);
	}
	
	private static void sanityCheck(CSVRecord row){
		if(validRecord(row)){
			Main.fIncrement();
			if((Integer.parseInt(row.get("YEAR")) == 2015) && (Integer.parseInt(row.get("MONTH")) == 1)){
				Main.addCarrier(row.get("UNIQUE_CARRIER"));
			}
			populateOutputMap(row);
		} else {
			Main.kIncrement();
		}
	}
		
	
	public void run(){
		try {	
			CSVFormat csvFormat = CSVFormat.DEFAULT.withIgnoreEmptyLines(true).withIgnoreSurroundingSpaces(true).withHeader();
			GZIPInputStream gis = new GZIPInputStream(new FileInputStream(Main.location+csvGzName));
			CSVParser parser = csvFormat.parse(new InputStreamReader(gis));

			for (CSVRecord row : parser) {
				if(row.size() == 110){
					sanityCheck(row);
				} else {
					Main.kIncrement();
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
