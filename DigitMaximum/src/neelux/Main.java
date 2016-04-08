package neelux;

public class Main {
	public static void main(String[] args){
		int X=223336226;
		StringBuilder xString = new StringBuilder();
		xString.append(X);
		System.out.println(xString.toString());
		
		int smallestDigit = 9;
		int smallestIndex = 0;
		int previousDigit = 10;
		for(int i=0; i<xString.length(); i++){
			int currentDigit = Character.getNumericValue(xString.charAt(i));
			if(currentDigit == previousDigit){
				if(currentDigit < smallestDigit){
					smallestDigit = currentDigit;
					smallestIndex = i;
				}
			}
			previousDigit = currentDigit;
		}
		System.out.println(xString.toString());
		System.out.println(smallestDigit);
		System.out.println(smallestIndex);
		xString.deleteCharAt(smallestIndex);
		System.out.println(xString.toString());
		
	}
}
