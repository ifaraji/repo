package bytebuffers;

public class ByteBuffer01 implements IByteBuffer {
	
	private byte b0;
	
	public ByteBuffer01(byte[] input){
		b0 = input[0];
	}
	
	@Override
	public byte[] getData() {
		return new byte[]{b0};
	}

}
