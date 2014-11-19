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
	
	public static IByteBuffer getByteBuffer(byte[] byteArray) {
		/*switch(byteArray.length){
		case 1: return new ByteBuffer01(byteArray);
		}*/
		String l = lpad(String.valueOf(byteArray.length),2,'0');
		l = "bytebuffers.ByteBuffer" + l;
		ClassLoader classLoader = ByteBufferI.class.getClassLoader();
		
		Class<IByteBuffer> aClass = null;
		try {
			aClass = (Class<IByteBuffer>) classLoader.loadClass(l);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Constructor<IByteBuffer> constructor = null;
		try {
			constructor = aClass.getConstructor(byte[].class);
		} catch (NoSuchMethodException | SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		IByteBuffer ibf = null;
		try {
			ibf = constructor.newInstance(byteArray);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ibf;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		byte[] b2 = new byte[]{126,126};
		byte[] b12 = new byte[]{127,127,127,127,127,127,127,127,127,127,127,127};
		
		IByteBuffer ibf = getByteBuffer(b2);
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