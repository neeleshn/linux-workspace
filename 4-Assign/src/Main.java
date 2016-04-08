// Author: Habiba Neelesh
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
	
    public static class CsvMapper extends Mapper<Object, Text, Text, Text> {

        Text customKey = new Text();
        Text customValue = new Text();
        int counter1 = 0;
        int counter2 = 0;

        private static int hhmmDiff(String arr, String dep) {
            if (arr.length() == 3) {
                arr = '0' + arr;
            }
            if (dep.length() == 3) {
                dep = '0' + dep;
            }
            if (Integer.parseInt(arr) > Integer.parseInt(dep)) {
                return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2))) * 60
                        + (Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
            } else {
                return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2)) + 24) * 60
                        + (Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
            }
        }

        public boolean validRow(String[] row) {
            try {
				// CRSArrTime and CRSDepTime should not be zero
                // timeZone % 60 should be 0

                String crsArrTime = row[40];
                String crsDepTime = row[29];
                int crsElapsedTime = Integer.parseInt(row[50]);

                if (crsArrTime.equals("") || crsDepTime.equals("") || crsArrTime.equals("0") || crsDepTime.equals("0")) {
                    return false;
                }

                int crsDiff = hhmmDiff(crsArrTime, crsDepTime);

                int timeZone = crsDiff - crsElapsedTime;

                if ((timeZone % 60) != 0) {
                    return false;
                }

				// AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
                int originAirportId = Integer.parseInt(row[11]);
                if (originAirportId <= 0) {
                    return false;
                }

                int originAirportSeqId = Integer.parseInt(row[12]);
                if (originAirportSeqId <= 0) {
                    return false;
                }

                int originCityMarketID = Integer.parseInt(row[13]);
                if (originCityMarketID <= 0) {
                    return false;
                }

                int originStateFips = Integer.parseInt(row[17]);
                if (originStateFips <= 0) {
                    return false;
                }

                int originWac = Integer.parseInt(row[19]);
                if (originWac <= 0) {
                    return false;
                }

                int destAirportId = Integer.parseInt(row[20]);
                if (destAirportId <= 0) {
                    return false;
                }

                int destAirportSeqId = Integer.parseInt(row[21]);
                if (destAirportSeqId <= 0) {
                    return false;
                }

                int destCityMarketId = Integer.parseInt(row[22]);
                if (destCityMarketId <= 0) {
                    return false;
                }

                int destStateFips = Integer.parseInt(row[26]);
                if (destStateFips <= 0) {
                    return false;
                }

                int destWac = Integer.parseInt(row[28]);
                if (destWac <= 0) {
                    return false;
                }

				// Origin, Destination,  CityName, State, StateName should not be empty
                String origin = row[14];
                if (origin.equals("")) {
                    return false;
                }

                String originCityName = row[15];
                if (originCityName.equals("")) {
                    return false;
                }

                String originStateAbr = row[16];
                if (originStateAbr.equals("")) {
                    return false;
                }

                String originStateName = row[18];
                if (originStateName.equals("")) {
                    return false;
                }

                String dest = row[23];
                if (dest.equals("")) {
                    return false;
                }

                String destCityName = row[24];
                if (destCityName.equals("")) {
                    return false;
                }

                String destStateAbr = row[25];
                if (destStateAbr.equals("")) {
                    return false;
                }

                String destStateName = row[27];
                if (destStateName.equals("")) {
                    return false;
                }

				// For flights that are not Cancelled:
                int cancelled = Integer.parseInt(row[47]);

                if (cancelled != 1) {

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

                    if (arrDelay > 0.0) {
                        if (arrDelay != arrDelayMinutes) {
                            return false;
                        }
                    }

                    if (arrDelay < 0.0) {
                        if (arrDelayMinutes != 0) {
                            return false;
                        }
                    }

                    if (arrDelayMinutes > 15.0) {
                        if (arrDel15 != 1) {
                            return false;
                        }
                    }
                }
            } catch (Exception e) {
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

            if (row.length == 110) {
                counter1++;
                if (validRow(row)) {
                    counter2++;
                    customKey.set(row[6]);
                    String customValueStr = row[0] + "-" + row[109] + "-" + row[51] + "-" + row[54];
                    customValue.set(customValueStr);
                    context.write(customKey, customValue);
                }
            }
        }
    }

    public static class CsvReducer extends Reducer<Text, Text, Text, Text> {

        Text customTextVal = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            boolean active = false;
            String eachValue;
            String[] valuesArray;
            ArrayList<String> reduceArray = new ArrayList<String>();

            for (Text val : values) {
                counter++;
                eachValue = val.toString();
                valuesArray = eachValue.split("-");
                if (valuesArray[0].equals("2015")) {
                    active = true;
                } else {
                    reduceArray.add(valuesArray[1] + "\t" + valuesArray[2] + "\t" + valuesArray[3]);
                }

            }
			Text value = new Text();
            if (active) {
                for (String val : reduceArray) {
                	value.set(val);
                    context.write(key, value);
                }
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