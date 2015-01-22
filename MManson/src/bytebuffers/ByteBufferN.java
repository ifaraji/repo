package bytebuffers;

public class ByteBufferN extends AbstractByteBuffer{

	byte[] b;
	
	public ByteBufferN(byte[] input){
		super();
		b = input.clone();
	}

	@Override
	public byte[] getData() {
		return b;
	}

}
