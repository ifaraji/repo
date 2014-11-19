package bytebuffers;


public class ByteBuffer02 implements IByteBuffer {
	
	private byte b0;
	private byte b1;
	
	public ByteBuffer02(byte[] input){
		b0 = input[0];
		b1 = input[1];
	}
	
	@Override
	public byte[] getData() {
		return new byte[]{b0,b1};
	}

}
