package com.tabops.helpers;

import java.util.Arrays;

public class QuickSort {

	// This class should not be instantiated.
	private QuickSort() {
	}

	/**
	 * Rearranges the array in ascending order, using the natural order.
	 * 
	 * @param a
	 *            the array to be sorted
	 */
	public static String[][] sort(String[][] a, int c) {
		int l = a.length;
		String[] ac = new String[l];
		int[] aci = new int[l]; 
		for (int i = 0; i < l; i++){
			ac[i] = a[i][c];
			aci[i] = i;
		}
		
		sort(ac, aci);
		
		String[][] o = new String[l][];		
		for (int i = 0; i < l; i++)
			o[i] = a[aci[i]];
		
		/*for (int i = 0; i < l; i++)
			a[i] = o[i];*/
		return o;
	}

	private static void sort(Comparable[] a, int[] rows) {
		//StdRandom.shuffle(a);
		sort(a, rows, 0, a.length - 1);
		assert isSorted(a);
	}

	// quicksort the subarray from a[lo] to a[hi]
	private static void sort(Comparable[] a, int[] rows, int lo, int hi) {
		if (hi <= lo)
			return;
		int j = partition(a, rows, lo, hi);
		sort(a, rows, lo, j - 1);
		sort(a, rows, j + 1, hi);
		assert isSorted(a, lo, hi);
	}

	// partition the subarray a[lo..hi] so that a[lo..j-1] <= a[j] <= a[j+1..hi]
	// and return the index j.
	private static int partition(Comparable[] a, int[] rows, int lo, int hi) {
		int i = lo;
		int j = hi + 1;
		Comparable v = a[lo];
		while (true) {

			// find item on lo to swap
			while (less(a[++i], v))
				if (i == hi)
					break;

			// find item on hi to swap
			while (less(v, a[--j]))
				if (j == lo)
					break; // redundant since a[lo] acts as sentinel

			// check if pointers cross
			if (i >= j)
				break;

			exch(a, rows, i, j);
		}

		// put partitioning item v at a[j]
		exch(a, rows, lo, j);

		// now, a[lo .. j-1] <= a[j] <= a[j+1 .. hi]
		return j;
	}

	/***********************************************************************
	 * Helper sorting functions
	 ***********************************************************************/

	// is v < w ?
	private static boolean less(Comparable v, Comparable w) {
		return (v.compareTo(w) < 0);
	}

	// exchange a[i] and a[j]
	private static void exch(Object[] a, int[] rows, int i, int j) {
		Object swap = a[i];
		a[i] = a[j];
		a[j] = swap;
		
		int s = rows[i]; 
		rows[i] = rows[j];
		rows[j] = s;
	}

	/***********************************************************************
	 * Check if array is sorted - useful for debugging
	 ***********************************************************************/
	private static boolean isSorted(Comparable[] a) {
		return isSorted(a, 0, a.length - 1);
	}

	private static boolean isSorted(Comparable[] a, int lo, int hi) {
		/*for (int i = lo + 1; i <= hi; i++)
			if (less(a[i], a[i - 1]))
				return false;*/
		return true;
	}

	public static void main(String[] args) {
		String[][] a = new String[][]{{"a","37","aa","aaa"}, {"b","2","bb","bbb"}, {"c","31","cc","ccc"}, {"d","15","dd","ddd"}, {"e","46","ee","eee"}};
		a = QuickSort.sort(a, 1);
		
		for (String[] c : a)
			System.out.println(Arrays.deepToString(c));
	}

}
