package bytebuffers;

public abstract class AbstractByteBuffer implements ByteBufferInterface{
	private int[] rows;
	private int C;
	
	public AbstractByteBuffer() {
	}
	
	public int[] getRows() {
		return rows;
	}
	
	public void addRow(int row) {
		rows[C++] = row;
	}

	public void createRows(int cap) {
		rows = new int[cap];
	}

}
