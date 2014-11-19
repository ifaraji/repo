package bytebuffers;


public class ByteBuffer03 implements IByteBuffer {
	
	private byte b0;
	private byte b1;
	private byte b2;
	
	public ByteBuffer03(byte[] input){
		b0 = input[0];
		b1 = input[1];
		b2 = input[2];
	}
	
	@Override
	public byte[] getData() {
		return new byte[]{b0,b1,b2};
	}

}
