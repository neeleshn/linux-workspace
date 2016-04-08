package vedant;

import vedant.sequential.CsvGzReader;

public class Assg00Main {

	public static void main(String[] args) {
		CsvGzReader cgr = new CsvGzReader();
		cgr.processGZipFile("one-month/323.csv.gz");
	}
}
