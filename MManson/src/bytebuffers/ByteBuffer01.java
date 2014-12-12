package bytebuffers;

public class ByteBuffer01 extends AbstractByteBuffer {
	
	private byte b0;
	
	public ByteBuffer01(byte[] input){
		super();
		b0 = input[0];
	}
	
	@Override
	public byte[] getData() {
		return new byte[]{b0};
	}

}
