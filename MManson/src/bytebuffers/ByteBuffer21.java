package bytebuffers;

public class ByteBuffer21 extends AbstractByteBuffer {
	private byte b0;
	private byte b1;
	private byte b2;
	private byte b3;
	private byte b4;
	private byte b5;
	private byte b6;
	private byte b7;
	private byte b8;
	private byte b9;
	private byte b10;
	private byte b11;
	private byte b12;
	private byte b13;
	private byte b14;
	private byte b15;
	private byte b16;
	private byte b17;
	private byte b18;
	private byte b19;
	private byte b20;
	
	public ByteBuffer21(byte[] input){
		super();
		b0 = input[0];
		b1 = input[1];
		b2 = input[2];
		b3 = input[3];
		b4 = input[4];
		b5 = input[5];
		b6 = input[6];
		b7 = input[7];
		b8 = input[8];
		b9 = input[9];
		b10 = input[10];
		b11 = input[11];
		b12 = input[12];
		b13 = input[13];
		b14 = input[14];
		b15 = input[15];
		b16 = input[16];
		b17 = input[17];
		b18 = input[18];
		b19 = input[19];
		b20 = input[20];
	}
	
	@Override
	public byte[] getData() {
		return new byte[]{b0,b1,b2,b3,b4,b5,b6,b7,b8,b9,b10,b11,b12,b13,b14,b15,b16,b17,b18,b19,b20};
	}	

}
