
public class Main {

	public static void main(String[] args){
		int[] A = {9,4,-3,-10};
		
		
		
//		if(A.length<1){
//			return -1;
//		}
		int sum = 0;
		for(int i = 0; i< A.length; i++ ){
			sum+=A[i];
		}
		int avg = sum/A.length;
		
		int max_value = Math.abs(A[0]-avg);
		int max_index = 0;
		int temp_value = 0;
		for(int i =0; i< A.length; i++){
			temp_value = Math.abs(A[i]-avg);
			if(max_value<temp_value){
				max_value = temp_value;
				max_index = i;
			}
		}
		System.out.println(max_index);
		System.out.println(Integer.toString(17, -7));
		System.out.println(Integer.toString(-17, -7));
	}
}
