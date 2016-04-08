package neelux;

public class Main {
	
	
	
	public static void main(String[] args){
		String input="dir1\n dir11\n dir12\n  pircture.jpeg\n  dir121\nfile1.txt\ndir2\n file2.gif";
		System.out.println(input);
		
		String pathTillNow = "";
		int sum=0;
		String[] ipArr = input.split("\n");
		
		for(int i=0; i<ipArr.length; i++){
			int spaceCount = 0;
			for(int j=0; j<ipArr[i].length(); j++){
				if(ipArr[i].charAt(j) == ' '){
					spaceCount++;
				} else {
					break;
				}
			}
			String trimStr = ipArr[i].trim();
			String[] pathArray = pathTillNow.split("/");
			pathTillNow = "";
			
			for(int j=1; j<=spaceCount; j++){
				pathTillNow+="/"+pathArray[j];
			}
			
			if(!trimStr.contains(".")){
				pathTillNow+="/"+trimStr;
			}
			
			try{
				String extension5 = trimStr.substring(trimStr.length()-5);
				String extension4 = trimStr.substring(trimStr.length()-4);
				if(extension4.equalsIgnoreCase(".png") || 
						extension4.equalsIgnoreCase(".gif") ||
						extension5.equalsIgnoreCase(".jpeg")){
					sum+=pathTillNow.length();
				}
			} catch (Exception e){
				
			}
		}
		System.out.println(sum);
	}
}
