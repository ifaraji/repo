package helpers;

public class IntArrayUtils {
	public static String intArrayToString(int[] a) {
		if (IsEmpty(a))
			return "";
		StringBuilder sBuilder = new StringBuilder();
		sBuilder.append(a[0]);
		for(int i = 1; i < a.length; i++) {
			sBuilder.append(',');
			sBuilder.append(a[i]);
		}
		return sBuilder.toString();	
	}
	
	public static boolean IsEmpty(int[] a) {
		if (a == null || a.length == 0)
			return true;
		return false;
	}
	
	public static int[] merge(int[] a, int[] b) {
    	if ( IsEmpty(a) && !IsEmpty(b) ) return b;
    	if ( !IsEmpty(a) && IsEmpty(b) ) return a;
    	if ( IsEmpty(a) && IsEmpty(b) ) return null;
    	
    	int[] result = new int[a.length + b.length];
    	
    	int i = 0; //result index
    	int j = 0; //a index
    	int k = 0; //b index   	
    	for(i = 0; i < result.length; i++) {
    		if (j == a.length) result[i] = b[k++];
    		else if (k == b.length) result[i] = a[j++];
    		else if (a[j] < b[k]) result[i] = a[j++];
    		else result[i] = b[k++];
    	}
    	return result;
	}
	
	public static int[] union(int[] a, int[] b) {
    	return merge(a, b);
	}
	
	public static int[] intersection(int[] a, int[] b) {
    	if ( IsEmpty(a) || IsEmpty(b) ) return new int[0];
    	
    	int[] tResult = new int[Math.max(a.length, b.length)];
    	int i = 0;
    	int j = 0;
    	int k = 0;
    	
    	while (i < a.length && j < b.length) {
	    	while (i < a.length && a[i] < b[j])	i++;
	    	
	    	while (j < b.length && i < a.length && a[i] > b[j])	j++;
	    	
	    	while (i < a.length && j < b.length && a[i] == b[j]){
	    		tResult[k] = a[i]; 
	    		i++;
	    		j++;
	    		k++;
	    	}
    	}
    	
    	int[] result = new int[k];
    	for (int h = 0; h < k; h++)
    		result[h] = tResult[h];
    	
    	return result;
	}
}
