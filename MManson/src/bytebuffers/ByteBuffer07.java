package bytebuffers;


public class ByteBuffer07 implements IByteBuffer{
	private byte b0;
	private byte b1;
	private byte b2;
	private byte b3;
	private byte b4;
	private byte b5;
	private byte b6;
	
	public ByteBuffer07(byte[] input){
		b0 = input[0];
		b1 = input[1];
		b2 = input[2];
		b3 = input[3];
		b4 = input[4];
		b5 = input[5];
		b6 = input[6];
	}
	
	@Override
	public byte[] getData() {
		return new byte[]{b0,b1,b2,b3,b4,b5,b6};
	}
}
