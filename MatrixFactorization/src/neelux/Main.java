package neelux;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {
	static double[][] trainMatrix = new double[943][1682];
	static double[][] predMatrix = new double[943][1682];
	static final double eeta = 0.01;
	static final int lambda = 100;
	static double[][] pu = new double[943][3];
	static double[][] qi = new double[1682][3];
	
	public static void readData() throws IOException{
		FileReader fr = new FileReader("u.data");
		BufferedReader br = new BufferedReader(fr);
		String line = null;
		
		//initialize pu and qi
		for(int i=0; i<943; i++){
			pu[i][0] = 1.0;
			pu[i][1] = 1.0;
			pu[i][2] = 1.0;
			qi[i][0] = 1.0;
			qi[i][1] = 1.0;
			qi[i][2] = 1.0;
		}
		for(int i=943; i<1682; i++){
			qi[i][0] = 1.0;
			qi[i][1] = 1.0;
			qi[i][2] = 1.0;
		}
		
		while((line = br.readLine()) != null){
			String[] lineArray = line.split("\t");
			int rowIndex = Integer.parseInt(lineArray[0])-1;
			int columnIndex = Integer.parseInt(lineArray[1])-1;
			trainMatrix[rowIndex][columnIndex] = Double.parseDouble(lineArray[2]);
		}
		br.close();
		fr.close();
	}
	
	public static void trainData(int fold){
		int rowStartIndex = (fold * 943)/5;
		int rowEndIndex = ((fold + 1) * 943)/5;
		for(int k=0; k<1000; k++){
			for(int i=0; i<943; i++){
				for(int j=0; j<1682; j++){
					if((trainMatrix[i][j] != 0) || ((i >= rowStartIndex) && (i < rowEndIndex))){
						double ruiCap = pu[i][0]*qi[j][0] + pu[i][1]*qi[j][1] + pu[i][2]*qi[j][2]; 
						pu[i][0]+= eeta*((trainMatrix[i][j]-ruiCap)*qi[i][0] - lambda*pu[i][0]);
						pu[i][1]+= eeta*((trainMatrix[i][j]-ruiCap)*qi[i][1] - lambda*pu[i][1]);
						pu[i][2]+= eeta*((trainMatrix[i][j]-ruiCap)*qi[i][2] - lambda*pu[i][2]);
						qi[i][0]+= eeta*((trainMatrix[i][j]-ruiCap)*pu[i][0] - lambda*qi[i][0]);
						qi[i][1]+= eeta*((trainMatrix[i][j]-ruiCap)*pu[i][1] - lambda*qi[i][1]);
						qi[i][2]+= eeta*((trainMatrix[i][j]-ruiCap)*pu[i][2] - lambda*qi[i][2]);
					}
				}
			}
			if(k%200 == 0){
				System.out.println(k);
			}
		}
	}
	
	public static void predictData(int fold){
		int rowStartIndex = fold * 943/5;
		int rowEndIndex = (fold + 1) * 943/5;
		for(int i=rowStartIndex; i<rowEndIndex; i++){
			for(int j=0; j<1682; j++){
				predMatrix[i][j]= pu[i][0]*qi[j][0] + pu[i][1]*qi[j][1] + pu[i][2]*qi[j][2];
			}
		}
	}
	
	public static double calculateMae(int fold){
		int count = 0;
		double sum = 0;
		int rowStartIndex = fold * 943/5;
		int rowEndIndex = (fold + 1) * 943/5;
		for(int i=rowStartIndex; i<rowEndIndex; i++){
			for(int j=0; j<1682; j++){
				if(trainMatrix[i][j] != 0){
					count++;
					sum+= Math.abs(predMatrix[i][j] - trainMatrix[i][j]);
				}
			}
		}
		return sum/count;
	}
	
	public static double calculateRmse(int fold){
		int count = 0;
		double sum = 0;
		int rowStartIndex = fold * 943/5;
		int rowEndIndex = (fold + 1) * 943/5;
		for(int i=rowStartIndex; i<rowEndIndex; i++){
			for(int j=0; j<1682; j++){
				if(trainMatrix[i][j] != 0){
					count++;
					sum+= Math.pow(predMatrix[i][j] - trainMatrix[i][j], 2);
				}
			}
		}
		return Math.sqrt(sum/count);
	}
	
	
	public static void main(String[] args) throws IOException{
		double mae=0;
		double rmse=0;
		readData();
		for(int i=0; i<5; i++){
			trainData(i);
			predictData(i);
			mae+= calculateMae(i);
			rmse+= calculateRmse(i);
		}
		mae = mae/5;
		rmse = rmse/5;
		System.out.println("Mean Absolute Error: "+Math.round(mae * 100.0)/100.0); // 3.53
		System.out.println("Root Mean Square Error: "+Math.round(rmse * 100.0)/100.0); //3.7
	}
}
