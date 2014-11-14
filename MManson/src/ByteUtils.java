
public class ByteUtils {
	public static int bitStringToInt(String input, int base, int step) {
		if (step == input.length()) return 0;
		int i = (int)input.charAt(input.length() - (step + 1)) - 48;
		return i * (int)Math.pow(base, step) + bitStringToInt(input, base, ++step);
	}
	
	public static String intToBitString(int input, int base) {
		StringBuilder sb = new StringBuilder();
		sb.append(input % base);
		if (input >= base)
			sb.insert(0,intToBitString(input/base, base));
		return sb.toString();
	}
	
	public static String lpad(String input, int paddedLength, char paddingCharacter) {
		StringBuilder sb = new StringBuilder(input);
		while(sb.length() < paddedLength)
			sb.insert(0, paddingCharacter);
		return sb.toString();
	}
	
	private static String substring(String input, int beginIndex, int endIndex) {
		if (endIndex >= input.length())
			return input.substring(beginIndex);
		else
			return input.substring(beginIndex, endIndex);
	}
	
	private static String pathToBitString(String path) {
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(index < path.length()) {
			char c = path.charAt(index);
			switch(c){
				case 'M': sb.append('0');break;
				case 'L': sb.append("10");break;
				case 'R': sb.append("11");break;		
			}
			index++;
		}
		return sb.toString();
	}
	
	public static byte[] bitStringToByteArray(String bitString) {
		int L = bitString.length();
		int N = (int)Math.ceil(L / 6d); 
		byte[] ba = new byte[N];
		
		int chunk = 0;
		int index = 0;
		while(index < ba.length) {
			String tPath = substring(bitString, chunk, chunk + 6);
			tPath = "1" + tPath;
			ba[index] = Byte.parseByte(tPath, 2);
			chunk += 6;
			index++;
		}	
		
		return ba;
	}
	
	public static byte[] pathToByteArray(String path) {
		String bits = pathToBitString(path);
		byte[] ba = bitStringToByteArray(bits);
		return ba;
	}
	
	private static String bitStringToPath(String input) {
		StringBuilder sb = new StringBuilder();
		int index = 1;
		while(index < input.length()) {
			if (index % 7 == 0)
				index++;
			char c = input.charAt(index);
			switch(c){
				case '0':sb.append('M');break;
				case '1': {
					if (++index % 7 == 0)
						index++;
					if (input.charAt(index) == '0')
						sb.append("L");
					else if (input.charAt(index) == '1')
						sb.append("R");
					break;
				}				
			}
			index++;
		}
				
		return sb.toString();
		
	}
	
	public static String byteArrayToPath(byte[] input) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < input.length; i++)
			sb.append(intToBitString(input[i], 2));				
		return bitStringToPath(sb.toString());
	}

}
