package com.tabops;

import java.util.Arrays;

import com.helpers.Stopwatch;
import com.tabops.helpers.QuickSort;

public class MergeJoin {
	
	public static String[][] apply(String[][] a, int ac, String[][] b, int bc) {
		return sortMerge(a, ac, b, bc);
	}
	
	private static String[][] sortMerge(String[][] a, int ac, String[][] b, int bc) {
		Stopwatch stopwatch = new Stopwatch();
		a = QuickSort.sort(a, ac); stopwatch.printElapsedtimeInMillis("sort-1");
		b = QuickSort.sort(b, bc); stopwatch.printElapsedtimeInMillis("sort-2");
		
		a = merge(a, ac, b, bc);
		stopwatch.printElapsedtimeInMillis("merge");
		return a;
	}
	
	private static String[][] merge(String[][] a, int ac, String[][] b, int bc) {
		if (a == null) return b;
		if (b == null) return a;
		int[][] key = new int[a.length + b.length][2];
		int i = 0, j = 0, k = 0, x = -1;
		while (i < key.length && j < a.length && k < b.length) {
			int cmp = a[j][ac].compareTo(b[k][bc]);
			if (cmp == 0) { key[i++] = new int[]{j+1, k+1}; if (x == -1) x = k; if ( (k + 1) < b.length) k++; else j++;}
			else if (cmp < 0) { j++; if (x != -1) k = x; x = -1;}
			else k++;
		}
		
		String[][] o = new String[i][];
		
		for (int y = 0; y < i; y++){
			o[y] = concatArrays(a[ key[y][0]-1 ], b[ key[y][1]-1 ]); 
		}
		
		return o;
	}
	
	private static String[] concatArrays(String[] a, String[] b){
		String[] c = new String[a.length + b.length];
		for(int i = 0; i < a.length; i++)
			c[i] = a[i];
		for(int i = a.length; i < b.length + a.length; i++)
			c[i] = b[i - a.length];		
		return c;
	}
	
	public static void main(String[] args) {
		String[][] a = new String[][]{{"a","10"}, {"b","20"}, {"c","30"}, {"d","40"}, {"e","50"}, {"f","60"}, {"g","70"}, {"h","70"}};
		String[][] b = new String[][]{{"aa","20"}, {"bb","20"}, {"cc","40"}, {"dd","40"}, {"ee","40"}, {"ff","40"}, {"gg","40"}/*, {"hh","60"}, {"ii","70"}*/};
		
		String[][] key = merge(a, 1, b, 1);
		
		/*for (int[] c : key){
			for (int d : c)
				System.out.print(d + "-");
			System.out.println();
		}*/
		
		for (String[] c : key)
			System.out.println(Arrays.deepToString(c));
	}
}
