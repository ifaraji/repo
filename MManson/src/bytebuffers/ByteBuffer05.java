package bytebuffers;


public class ByteBuffer05 extends AbstractByteBuffer{
	private byte b0;
	private byte b1;
	private byte b2;
	private byte b3;
	private byte b4;
	
	public ByteBuffer05(byte[] input){
		super();
		b0 = input[0];
		b1 = input[1];
		b2 = input[2];
		b3 = input[3];
		b4 = input[4];
	}
	
	@Override
	public byte[] getData() {
		return new byte[]{b0,b1,b2,b3,b4};
	}
}
