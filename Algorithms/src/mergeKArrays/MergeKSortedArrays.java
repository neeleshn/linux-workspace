package mergeKArrays;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

public class MergeKSortedArrays {
	
	public static ArrayList<Integer> Merge(int[][] arrays)
	{
		ArrayList<Integer> output = new ArrayList<Integer>();
		PriorityQueue<ListNode> pq= new PriorityQueue<ListNode>(arrays.length,new Comparator<ListNode>() {
			public int compare(ListNode a, ListNode b) {
				if (a.val > b.val)
					return 1;
				else if(a.val == b.val)
					return 0;
				else 
					return -1;
			}
		});
		
		return output;
	}
	public static void main(String args[])
	{
		int[][] input = {{1,4,7,10,11},{2,5,8,12},{3,6,9}};
		ArrayList<Integer> output = Merge(input);
		for(int i : output)
			System.out.println(i+"\t");
	}
}
