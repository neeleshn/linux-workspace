
import java.io.IOException;
import java.text.ParseException;
//import java.text.DateFormat;
//import java.text.FieldPosition;
//import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
	
	public static class CsvMapper  extends Mapper<Object, Text, Text, Text> {
		Text customKey = new Text();
		Text customValue = new Text();
		
		private static int hhmmDiff (String arr, String dep){
			if(arr.length()==3){
				arr='0'+arr;
			}
			if(dep.length()==3){
				dep='0'+dep;
			}
			if (Integer.parseInt(arr) > Integer.parseInt(dep)){
				return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2))) * 60 +
						(Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
			} else {
				return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2)) + 24) * 60 +
						(Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
			}
		}
		
		
	    public boolean validRow(String[] row){
	    	try{
				// CRSArrTime and CRSDepTime should not be zero
				// timeZone % 60 should be 0

				String crsArrTime = row[40];
				String crsDepTime = row[29];
				int crsElapsedTime = Integer.parseInt(row[50]);
				
				if (crsArrTime.equals("") || crsDepTime.equals("") || crsArrTime.equals("0") || crsDepTime.equals("0")){
					return false;
				}
				
				int crsDiff = hhmmDiff(crsArrTime, crsDepTime);
				
				int timeZone = crsDiff - crsElapsedTime;
				
				if ((timeZone % 60) != 0){
					return false;
				}
				
				
				// AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
				
				int originAirportId = Integer.parseInt(row[11]);
				if (originAirportId <= 0) return false;
				
				int originAirportSeqId = Integer.parseInt(row[12]);
				if (originAirportSeqId <= 0) return false;
				
				int originCityMarketID = Integer.parseInt(row[13]);
				if (originCityMarketID <= 0) return false;
				
				int originStateFips = Integer.parseInt(row[17]);
				if (originStateFips <= 0) return false;
				
				int originWac = Integer.parseInt(row[19]);
				if (originWac <= 0) return false;
				
				int destAirportId = Integer.parseInt(row[20]);
				if (destAirportId <= 0) return false;
				
				int destAirportSeqId = Integer.parseInt(row[21]);
				if (destAirportSeqId <= 0) return false;
				
				int destCityMarketId = Integer.parseInt(row[22]);
				if (destCityMarketId <= 0) return false;
				
				int destStateFips = Integer.parseInt(row[26]);
				if (destStateFips <= 0) return false;
				
				int destWac = Integer.parseInt(row[28]);
				if (destWac <= 0) return false;
				
				
				// Origin, Destination,  CityName, State, StateName should not be empty

				String origin = row[14];
				if (origin.equals("")) return false;
				
				String originCityName = row[15];
				if (originCityName.equals("")) return false;
				
				String originStateAbr = row[16];
				if (originStateAbr.equals("")) return false;
				
				String originStateName = row[18];
				if (originStateName.equals("")) return false;
				
				String dest = row[23];		
				if (dest.equals("")) return false;
				
				String destCityName = row[24];
				if (destCityName.equals("")) return false;
				
				String destStateAbr = row[25];
				if (destStateAbr.equals("")) return false;
				
				String destStateName = row[27];
				if (destStateName.equals("")) return false;
				
				
				// For flights that are not Cancelled:
				
				int cancelled = Integer.parseInt(row[47]);		
				
				if (cancelled != 1){			
					
					String arrTime = row[41];
					String depTime = row[30];
					int actualElapsedTime = Integer.parseInt(row[51]);
					
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
					
					float arrDelay = Float.parseFloat(row[42]);
					float arrDelayMinutes = Float.parseFloat(row[43]);
					float arrDel15 = Float.parseFloat(row[44]);
					
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
				
				//Extra checks for Assignment 5
				String carrier = row[6];
				if (carrier.length()!=2){
					return false;
				}
				
				String date = row[5];
				if(date.length()!=10){
					return false;
				}
				
				int actArrLen = row[41].length();
				int actDepLen = row[30].length();
				
				if (!(actArrLen == 3 || actArrLen == 4)) {
					return false;
				}
				if (!(actDepLen == 3 || actDepLen == 4)) {
					return false;
				}
				
			} catch(Exception e) {
				return false;
			}
	    	return true;
	    }
	    
	    public String[] parseCSVLine(String line) {
	        List<String> values = new ArrayList<String>();
	        StringBuffer sb = new StringBuffer();
	        boolean inQuote = false;
	        char curChar;
	        for (int i = 0; i < line.length(); i++) {
	            curChar = line.charAt(i);
	            if (inQuote) {
	                if (curChar == '"') {
	                    inQuote = false;
	                } else {
	                    sb.append(curChar);
	                }
	            } else {
	                if (curChar == '"') {
	                    inQuote = true;
	                } else if (curChar == ',') {
	                    values.add(sb.toString());
	                    sb = new StringBuffer();
	                } else {
	                    sb.append(curChar);
	                }
	            }
	        }
	        values.add(sb.toString());  // last field
	        return values.toArray(new String[1]);
	    }

	    
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	String eachLine = value.toString();
	    	String[] row = parseCSVLine(eachLine);
	    	
	    	if(row.length==110){
	    		if(validRow(row)){
	    			customKey.set(row[6]+"\t"+row[14]);
	    			customValue.set("G"+"\t"+row[5]+"\t"+row[29]+"\t"+row[30]);
	    			context.write(customKey,customValue);
	    			
	    			customKey.set(row[6]+"\t"+row[23]);
	    			customValue.set("F"+"\t"+row[5]+"\t"+row[40]+"\t"+row[41]);
	    			context.write(customKey,customValue);
	    		}
	    	}
	    }
	}
	
	
	public static class CsvReducer extends Reducer<Text,Text,Text,Text> {
		
		static long half_hour = 1800000;
		static long six_hours = 21600000;
		
		public static boolean connectionMissed(Date sch_dep, Date actual_dep, Date sch_arr, Date actual_arr){
			
			long sch_time_diff = sch_dep.getTime() - sch_arr.getTime();
			long actual_time_diff = actual_dep.getTime() - actual_arr.getTime();
			
			if(sch_time_diff <= six_hours && sch_time_diff >= half_hour){
				
				if(actual_time_diff < half_hour) {
					return true;
				}	
			}
			return false;
		}
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			ArrayList<String> valuesList = new ArrayList<>();
			HashMap<Integer, Integer> yearlyCount = new HashMap<Integer, Integer>();
			
			int counter = 0;
			try{
				for(Text val : values){
					valuesList.add(val.toString());
				}
				System.out.println("######################### : "+valuesList.size());
				for (String val1 : valuesList) {
					String[] valArray1= val1.split("\t");
					if(valArray1[0].equals("F")){
						String sch_arr_string = valArray1[2];
						String actual_arr_string = valArray1[3];
					
						if(sch_arr_string.length()==3){
							sch_arr_string= "0"+sch_arr_string;
						}
						if(actual_arr_string.length()==3){
							actual_arr_string= "0"+actual_arr_string;
						}
						
						sch_arr_string = valArray1[1] + " "+ sch_arr_string;
						actual_arr_string = valArray1[1] +" "+ actual_arr_string;
					
						SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hhmm");
						Date sch_arr = formatter.parse(sch_arr_string);
						Date actual_arr = formatter.parse(actual_arr_string);
						Calendar cal = Calendar.getInstance();
						cal.setTime(sch_arr);
						int year = cal.get(Calendar.YEAR);
						
						counter++;
						System.out.print(counter+"\t");
						for (String val2 : valuesList) {
							
							String[] valArray2 = val2.split("\t");
							if(valArray2[0].equals("G")){
								String origin = valArray2[1];
								String sch_dep_string = valArray2[2];
								String actual_dep_string = valArray2[3];
								
								if(sch_dep_string.length()==3){
									sch_dep_string= "0"+sch_dep_string;
								}
								if(actual_dep_string.length()==3){
									actual_dep_string = "0"+actual_dep_string;
								}
								
								sch_dep_string = valArray1[1] +" "+ sch_dep_string;
								actual_dep_string = valArray1[1] +" "+ actual_dep_string;
								
								Date sch_dep = formatter.parse(sch_dep_string);
								Date actual_dep = formatter.parse(actual_dep_string);
								
								if(connectionMissed(sch_dep, actual_dep, sch_arr, actual_arr)){
									if(yearlyCount.containsKey(year)){
										int tempCount = yearlyCount.get(year);
										yearlyCount.put(year, ++tempCount);
									} else {
										yearlyCount.put(year, 1);
									}
								}	
							}
						}
					}
				}
			} catch (ParseException e){
				System.out.println(e.getMessage());
			}
			
			for(Map.Entry<Integer, Integer> entry : yearlyCount.entrySet()){
				Text customValue = new Text(entry.getKey()+"\t"+entry.getValue());
				context.write(key,customValue);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "csv");
	    job.setJarByClass(Main.class);
	    job.setMapperClass(CsvMapper.class);
	    job.setReducerClass(CsvReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
