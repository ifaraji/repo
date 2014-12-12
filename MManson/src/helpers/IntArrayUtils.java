package helpers;

public class IntArrayUtils {
	public static String intArrayToString(int[] a) {
		if (IsNull(a))
			return "";
		StringBuilder sBuilder = new StringBuilder();
		sBuilder.append(a[0]);
		for(int i = 1; i < a.length; i++) {
			sBuilder.append(',');
			sBuilder.append(a[i]);
		}
		return sBuilder.toString();	
	}
	
	public static boolean IsNull(int[] a) {
		if (a == null || a.length == 0)
			return true;
		return false;
	}
}
