package neelux;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {
	static double[][] trainMatrix = new double[943][1682];
	static double[][] predMatrix = new double[943][1682];
	static double[][] testMatrix = new double[943][1682];
	static final double eeta = 0.001;
	static final double lambda = 0.02;
	static int m = 2;
	static double[][] pu = new double[943][2];
	static double[][] qi = new double[2][1682];
	
	public static void init(){
		for(int i=0; i<943; i++){
			for(int j=0; j<1682; j++){
				trainMatrix[i][j]=0.0;
				predMatrix[i][j]=0.0;
				testMatrix[i][j]=0.0;
			}
		}
		for(int i=0; i<943; i++){
			pu[i][0] = 1.0;
			pu[i][1] = 1.0;
			qi[0][i] = 1.0;
			qi[1][i] = 1.0;
		}
		for(int i=943; i<1682; i++){
			qi[0][i] = 1.0;
			qi[1][i] = 1.0;
		}
	}
	
	public static void readData(int fold) throws IOException{
		FileReader fr = new FileReader("u.data");
		BufferedReader br = new BufferedReader(fr);
		String line = null;
		int lineNumber = 0;
		int foldStartIndex = (fold * 100000)/5;
		int foldEndIndex = ((fold + 1) * 100000)/5;
		
		while((line = br.readLine()) != null){
			String[] lineArray = line.split("\t");
			int rowIndex = Integer.parseInt(lineArray[0])-1;
			int columnIndex = Integer.parseInt(lineArray[1])-1;
			
			if(lineNumber<foldStartIndex || lineNumber>foldEndIndex){
				trainMatrix[rowIndex][columnIndex] = Double.parseDouble(lineArray[2]);
			} else {
				predMatrix[rowIndex][columnIndex] = Double.parseDouble(lineArray[2]);
				testMatrix[rowIndex][columnIndex] = Double.parseDouble(lineArray[2]);
			}
			lineNumber++;
		}
		br.close();
		fr.close();
	}
	
	public static void trainData(){
		boolean converged = false;
		double newObjective = 0.0;
		double oldObjective = 0.0;
		while(!converged) {
			for(int i=0; i<943; i++){
				for(int j=0; j<1682; j++){
					updatePQ(i,j);
				}
			}
			oldObjective = newObjective;
			newObjective = calObjective();
			double diff = Math.abs(newObjective - oldObjective);
			if(diff < 0.01){
				converged= true;
			}
		}
	}
	
	public static double calObjective(){
		double objective = 0.0;
		for(int i=0; i<943; i++){
			for(int j=0; j<1682; j++){
				if(trainMatrix[i][j] > 0.0){
					double ruiCap = 0.0;
					for(int k=0; k<m; k++){
						ruiCap += pu[i][k]*qi[k][j];
					}
					double diff = trainMatrix[i][j]-ruiCap; 
					objective += Math.pow(diff, 2);
					for(int k=0; k<m; k++){
						objective += (lambda/2) * (Math.pow(pu[i][k], 2) + Math.pow(qi[k][j], 2));
					}
				}
			}
		}
		return objective;
	}
	
	public static void updatePQ(int i, int j){
		if(trainMatrix[i][j] != 0.0){
			double ruiCap = 0.0;
			for(int k=0; k<m; k++){
				ruiCap += pu[i][k]*qi[k][j];
			}
			double diff = trainMatrix[i][j]-ruiCap;
			for(int k=0; k<m; k++){
				pu[i][k] += eeta* (2*diff*qi[k][j] - lambda*pu[i][k]);
				qi[k][j] += eeta* (2*diff*pu[i][k] - lambda*qi[k][j]);
			}
		}
	}
	
	public static void predictData(){
		for(int i=0; i<943; i++){
			for(int j=0; j<1682; j++){
				if(testMatrix[i][j] != 0.0) {
					predMatrix[i][j]= pu[i][0]*qi[0][j] + pu[i][1]*qi[1][j];
					//System.out.println(i+"\t,\t"+j+"\t,\t"+predMatrix[i][j]+"\t,\t"+pu[i][0]);
				}
			}
		}
	}
	
	public static double calculateMae(){
		double sum = 0;
		for(int i=0; i<943; i++){
			for(int j=0; j<1682; j++){
				if(testMatrix[i][j] != 0.0){
					sum+= Math.abs(predMatrix[i][j] - testMatrix[i][j]);
				}
			}
		}
		return sum/20000;
	}
	
	public static double calculateRmse(){
		double sum = 0;
		
		for(int i=0; i<943; i++){
			for(int j=0; j<1682; j++){
				if(testMatrix[i][j] != 0.0){
					sum+= Math.pow(predMatrix[i][j] - testMatrix[i][j], 2);
				}
			}
		}
		return Math.sqrt(sum/20000);
	}
	
	
	public static void main(String[] args) throws IOException{
		double mae=0;
		double rmse=0;
		for(int i=0; i<5; i++){
			init();
			readData(i);
			trainData();
			predictData();
			mae+= calculateMae();
			rmse+= calculateRmse();
		}
		mae = mae/5;
		rmse = rmse/5;
		System.out.println("Mean Absolute Error: "+Math.round(mae * 100.0)/100.0); // 0.75
		System.out.println("Root Mean Square Error: "+Math.round(rmse * 100.0)/100.0); //0.95
	}
}
