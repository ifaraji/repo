package bytebuffers;


public class ByteBuffer06 extends AbstractByteBuffer {
	
	private byte b0;
	private byte b1;
	private byte b2;
	private byte b3;
	private byte b4;
	private byte b5;
	
	public ByteBuffer06(byte[] input){
		super();
		b0 = input[0];
		b1 = input[1];
		b2 = input[2];
		b3 = input[3];
		b4 = input[4];
		b5 = input[5];
	}
	
	@Override
	public byte[] getData() {
		return new byte[]{b0,b1,b2,b3,b4,b5};
	}

}
