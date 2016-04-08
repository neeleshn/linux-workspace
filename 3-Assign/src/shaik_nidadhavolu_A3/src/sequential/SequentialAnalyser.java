// Author: Habiba Neelesh
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.Comparator;
import java.util.zip.GZIPInputStream;

public class SequentialAnalyser {

    public static int k = 0, f = 0;
    public static HashSet<String> activeCarriers = new HashSet<String>();
    public static HashSet<String> topCarriers = new HashSet<String>();
    public static HashMap<String, ArrayList<Float>> allPricesMap = new HashMap<String, ArrayList<Float>>();
    public static HashMap<String, ArrayList<Float>> monthCarrierMap = new HashMap<String, ArrayList<Float>>();
    public static String location;
    public static int toCalculate;

    public static void addCarrier(String carrierId) {
        activeCarriers.add(carrierId);
    }

    private static boolean validRecord(String[] row) {
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

                // if ArrDelay > 0 then ArrDelay should equal toIndex ArrDelayMinutes
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

    private static void populateOutputMap(String[] row) {
        String carrierId = row[6];
        String month = row[2];
        float avgPrice = Float.parseFloat(row[109]);
        allPopulate(carrierId, avgPrice);
        monthPopulate(carrierId + "-" + month, avgPrice);
    }

    public static void allPopulate(String carrierId, float avgPrice) {

        if (allPricesMap.containsKey(carrierId)) {
            ArrayList<Float> a1 = allPricesMap.get(carrierId);
            a1.add(avgPrice);
            allPricesMap.put(carrierId, a1);
        } else {
            ArrayList<Float> a1 = new ArrayList<>();
            a1.add(avgPrice);
            allPricesMap.put(carrierId, a1);
        }
    }

    public static void monthPopulate(String carrierId, float avgPrice) {

        if (monthCarrierMap.containsKey(carrierId)) {
            ArrayList<Float> a1 = monthCarrierMap.get(carrierId);
            a1.add(avgPrice);
            monthCarrierMap.put(carrierId, a1);
        } else {
            ArrayList<Float> a1 = new ArrayList<>();
            a1.add(avgPrice);
            monthCarrierMap.put(carrierId, a1);
        }
    }

    private static void sanityCheck(String[] row) {
        if (validRecord(row)) {
            if ((Integer.parseInt(row[0]) == 2015) && (Integer.parseInt(row[2]) == 1)) {
                addCarrier(row[6]);
            }
            populateOutputMap(row);
        }
    }

    public static String[] parseCSVLine(String line) {
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

    public static float quickSelectMedian(ArrayList<Float> array, int k) {
        if (array.size() == 0) {
            return 0;
        }
        if (array.size() == 1) {
            return array.get(0);
        }
        int fromIndex = 0;
        int toIndex = array.size() - 1;
        try {
            while (fromIndex < toIndex) {
                int r = fromIndex;
                int w = toIndex;
                float midIndex = array.get((r + w) / 2);
                while (r < w) {
                    if (array.get(r) >= midIndex) {
                        float tmp = array.get(w);
                        array.set(w, array.get(r));
                        array.set(r, tmp);
                        w--;
                    } else {
                        r++;
                    }
                }
                if (array.get(r) > midIndex) {
                    r--;
                }
                if (k <= r) {
                    toIndex = r;
                } else {
                    fromIndex = r + 1;
                }
            }
            return array.get(k);

        } catch (Exception e) {
            return 0;
        }
    }

    public static void main(String[] args) throws Exception {

        try {
            location = args[0];
            toCalculate = Integer.parseInt(args[1]);
        } catch (Exception e) {
            location = "resources/all";
            toCalculate = 0;
        }

        File inputDir = new File(location);
        File[] allCsvGz = inputDir.listFiles();

        int filesCount = allCsvGz.length;

        for (int i = 0; i < filesCount; i++) {
            String csvGzName = allCsvGz[i].getName();
            try {
                GZIPInputStream gis = new GZIPInputStream(new FileInputStream(location + csvGzName));
                BufferedReader br = new BufferedReader(new InputStreamReader(gis));
                String eachLine = br.readLine();
                eachLine = br.readLine();
                while (eachLine != null) {
                    String[] row = parseCSVLine(eachLine);
                    if (row.length == 110) {
                        sanityCheck(row);
                    }
                    eachLine = br.readLine();
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        List<Map.Entry<String, ArrayList<Float>>> list = new LinkedList<Map.Entry<String, ArrayList<Float>>>(allPricesMap.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<String, ArrayList<Float>>>() {
            public int compare(Map.Entry<String, ArrayList<Float>> o1, Map.Entry<String, ArrayList<Float>> o2) {
                if (o2.getValue().size() > o1.getValue().size()) {
                    return 1;
                } else {
                    return -1;
                }
            }
        });

        int counter = 1;
        for (Map.Entry<String, ArrayList<Float>> entry : list) {
            if (activeCarriers.contains(entry.getKey())) {
                topCarriers.add(entry.getKey());
                if (counter == 10) {
                    break;
                }
                counter++;
            }
        }

        Iterator<Entry<String, ArrayList<Float>>> itr = monthCarrierMap.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, ArrayList<Float>> entry = (Map.Entry<String, ArrayList<Float>>) itr.next();
            String[] keyArray = entry.getKey().split("-");
            String carrierId = keyArray[0];
            String month = keyArray[1];
            if (topCarriers.contains(carrierId)) {
                ArrayList<Float> allPrices = entry.getValue();
                int listSize = allPrices.size();
                if (toCalculate == 0) {
                    float sum = 0;
                    for (float eachPrice : allPrices) {
                        sum += eachPrice;
                    }
                    float meanPrice = sum / listSize;
                    System.out.println(month + " \t " + carrierId + " \t " + meanPrice);

                } else if (toCalculate == 1) {
                    Collections.sort(allPrices);
                    float medianPrice = 0;
                    if (listSize % 2 == 0) {
                        medianPrice = (allPrices.get(listSize / 2) + allPrices.get((listSize - 1) / 2)) / 2;
                    } else {
                        medianPrice = allPrices.get((listSize - 1) / 2);
                    }
                    System.out.println(month + " \t " + carrierId + " \t " + medianPrice);
                } else {
                    float median = quickSelectMedian(allPrices, allPrices.size() / 2);
                    System.out.println(month + " \t " + carrierId + " \t " + median);
                }
            }
        }
    }
}
