package helpers;

public class CharArrayUtil {
	public static char[] substring(char[] a, int beginIndex) {
		char[] result = new char[a.length - beginIndex];
		for (int i = 0; i < result.length; i++)
			result[i] = a[i + beginIndex];
		return result;
	}
	
	public static char[] substring(char[] a, int beginIndex, int endIndex) {
		char[] result = new char[endIndex - beginIndex];
		for (int i = 0; i < result.length; i++)
			result[i] = a[i + beginIndex];
		return result;
	}
	
	/**
	 * longest common prefix
	 * linear(worst case) / usually sub-linear (because we don't always have to iterate all chars)
	 * @param str1 String input string 1 
	 * @param str2 String input string 2 
	 * @return the longest common prefix of the two input strings
	 */
	public static int lcp(char[] str1, char[] str2) 
	{
		int N = Math.min(str1.length, str2.length);
		for (int i = 0; i < N; i++)
			if (str1[i] != str2[i])
				return i;
		return N;
	}  
	
	public static boolean bigger(char[] str1, char[] str2) {
		int i = 0;
		int N = Math.max(str1.length, str2.length);
		while(str1[i] == str2[i] && i < N) 
			i++;
		
		if (i == N)
			return str1.length > str2.length;  

		return str1[i] > str2[i];
	}

	public static boolean smaller(char[] str1, char[] str2) {
		int i = 0;
		int N = Math.max(str1.length, str2.length);
		while(str1[i] == str2[i] && i < N) 
			i++;
		
		if (i == N)
			return str1.length < str2.length;  
		
		return str1[i] < str2[i];
	}

	
	public static void main(String[] args) {
		String s = "abcdefghijk";
		char[] a = {'a','b','c','d','e','f','g','h','i','j','k'};
		
		System.out.println(s.substring(2));
		System.out.println(substring(a, 2));

		System.out.println(s.substring(2,4));
		System.out.println(substring(a, 2, 4));
	}
}
