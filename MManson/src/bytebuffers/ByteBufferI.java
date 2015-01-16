package bytebuffers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ByteBufferI {
	private static String lpad(String input, int paddedLength, char paddingCharacter) {
		StringBuilder sb = new StringBuilder(input);
		while(sb.length() < paddedLength)
			sb.insert(0, paddingCharacter);
		return sb.toString();
	}
	
	public static ByteBufferInterface getByteBuffer(byte[] byteArray) {
		/*switch(byteArray.length){
		case 1: return new ByteBuffer01(byteArray);
		}*/
		String l = lpad(String.valueOf(byteArray.length),2,'0');
		l = "bytebuffers.ByteBuffer" + l;
		ClassLoader classLoader = ByteBufferI.class.getClassLoader();
		
		Class<ByteBufferInterface> aClass = null;
		try {
			aClass = (Class<ByteBufferInterface>) classLoader.loadClass(l);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		Constructor<ByteBufferInterface> constructor = null;
		try {
			constructor = aClass.getConstructor(byte[].class);
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		
		ByteBufferInterface ibf = null;
		try {
			ibf = constructor.newInstance(byteArray);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
		}
		
		return ibf;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		byte[] b2 = new byte[]{126,126};
		byte[] b12 = new byte[]{127,127,127,127,127,127,127,127,127,127,127,127};
		
		ByteBufferInterface ibf = getByteBuffer(b2);
		byte[] b = ibf.getData();
		for(int i = 0; i < b.length; i++)
			System.out.print(b[i] + ", ");
		System.out.println();
		ibf = getByteBuffer(b12);
		b = ibf.getData();
		for(int i = 0; i < b.length; i++)
			System.out.print(b[i] + ", ");
	}
}
