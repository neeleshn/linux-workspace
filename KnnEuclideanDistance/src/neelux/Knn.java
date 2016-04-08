package neelux;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;

/*
 * @Author: Neelesh Nidadhavolu 
 */
public class Knn {

	public static void main(String[] args) throws FileNotFoundException {
		
		List<Instance> instances = DataSet.readDataSet("data.txt");
		Collections.shuffle(instances);
		
	}

}
