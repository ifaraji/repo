package bytebuffers;


public class ByteBuffer04 implements IByteBuffer {
	
	private byte b0;
	private byte b1;
	private byte b2;
	private byte b3;
	
	public ByteBuffer04(byte[] input){
		b0 = input[0];
		b1 = input[1];
		b2 = input[2];
		b3 = input[3];
	}
	
	@Override
	public byte[] getData() {
		return new byte[]{b0,b1,b2,b3};
	}

}
