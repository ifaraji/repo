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

	
	public static void main(String[] args) {
		String s = "abcdefghijk";
		char[] a = {'a','b','c','d','e','f','g','h','i','j','k'};
		
		System.out.println(s.substring(2));
		System.out.println(substring(a, 2));

		System.out.println(s.substring(2,4));
		System.out.println(substring(a, 2, 4));
	}
}
