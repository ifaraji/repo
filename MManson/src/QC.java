import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;


public class QC implements Serializable{

	private static final long serialVersionUID = 1L;

    public static class Column implements Serializable{
		private static final long serialVersionUID = 1L;
		private String name;
        private IdeenTrieC col;
        //private Column next; <LL impl>
    }

    //private Column first;    // beginning of bag <LL impl>
    private int M; 		     //number of columns
    
    private Column[] columns;
       
    public QC(String[] cols, int numOfRows){   
    	M = cols.length;
    	columns = new Column[M];
    	
    	for(int i = 0; i < M; i++){
    		columns[i] = new Column();
    		columns[i].name = cols[i];
    		columns[i].col = new IdeenTrieC(numOfRows, false);
    	}
    }
    
    public QC(String[] cols, int numOfRows, int uniqueColumnIndex){   
    	M = cols.length;
    	columns = new Column[M];
    	
    	for(int i = 0; i < M; i++){
    		columns[i] = new Column();
    		columns[i].name = cols[i];
    		columns[i].col = new IdeenTrieC(numOfRows, i == uniqueColumnIndex ? true : false);
    	}
    }
    
    /*public QC(String[] cols, int numOfRows, int uniqueColumnIndex, int[] compressedColsIndexes){   
    	M = cols.length;
    	columns = new Column[M];
    	
    	for(int i = 0; i < M; i++){
    		columns[i] = new Column();
    		columns[i].name = cols[i];
    		boolean compressed = false;
    		for (int j = 0; j < compressedColsIndexes.length; j++)
    			if (i == compressedColsIndexes[j])
    				compressed = true;
    		columns[i].col = new IdeenTrieC(numOfRows, i == uniqueColumnIndex ? true : false, compressed);
    	}
    }*/
    
    public QC insert(int columnId, String columnValue, int rowIndex) {
    	columns[columnId].col.insert(columnValue, rowIndex);
    	return this;
    }
    
    public void finalize() {
    	for(int i = 0; i < M; i++){
    		columns[i].col.finalize();
    	}
    }
    
    public int numOfCols() {
    	return M;
    }
    
    public String getColumnName(int columnIndex) {
    	return columns[columnIndex].name;
    }
    
    public Column[] getColumns() {
    	return columns;
    }
      
    public String[] getRow(int rowNum){
    	String[] row = new String[M]; 
    	for(int i = 0; i < M; i++)
    		row[i] = columns[i].col.getRowValue(rowNum);
    	return row;
    }
    
    public String[][] getRows(int[] rows){
    	String[][] rowSet = new String[rows.length][M];    	
    	for(int i = 0; i < rows.length; i++) {
    		rowSet[i] = getRow(rows[i]);
    	}
    	return rowSet;
    } 
    
    public int[] getRows(int columnIndex, String key){    	
    	int[] a = columns[columnIndex].col.getRows(key);
    	return a;
    }
    
    public int[] getRows(int columnIndex, String[] key){    	
    	int[] a = columns[columnIndex].col.getRows(key);
    	return a;
    }    
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
    	QC qc;
		Stopwatch stopwatch = new Stopwatch();
		
		String qcTableName = "..\\..\\my_mrps_sra_item_lvl1.dat"; 
		//String qcTableName = "..\\..\\my_mrps_sra_lvl2_sum.dat";
		
		File f = new File(qcTableName);
		if (f.exists()) {
			FileInputStream file = new FileInputStream(qcTableName);
	        ObjectInputStream in = new ObjectInputStream(file);
	        qc = (QC) in.readObject();
	        in.close();
	        file.close();
	        System.out.println("Deserialized...");
	        stopwatch.printElapsedtimeAndReset();
	        
	    	/*for(int i = 0; i < qc.M; i++){
		        FileOutputStream newFile = new FileOutputStream("col"+String.valueOf(i)+".dat");
				ObjectOutputStream out = new ObjectOutputStream(newFile);
				out.writeObject(qc.columns[i]);
				out.close();
				newFile.close();
	    	}*/
		}
		else {
			DataLoader dl = DataLoader.getInstance("..\\..\\mmsil1.csv");	
			//qc = new QC(new String[]{"tpc", "category_code", "brand", "product_type", "colour", "size1"}, dl.numOfRows());
			qc = new QC(new String[]{"ITEM", "TPC", "CATEGORY_CODE", "CLASS_GROUP", "CLASS", "SUBCLASS", "BRAND", "COLOUR_IND",
									"SIZE1_IND", "SIZE2_IND", "ONLINE_IND", "STATUS", "STATUS_DESC", "ITEM_NAME", "ITEM_SHORT_DESC",
									"ITEM_LONG_DESC", "MIN_PRICE", "MAX_PRICE", "IMAGE_ADDR"}, dl.numOfRows(), 0/*, new int[] {13,14,15,16,17,18}*/);	
			int rowCount = 0;
			try {
				while(dl.next()){ 		
					rowCount++;
					String[] row = dl.getCurrentRow();
					for (int i = 0; i < qc.M; i++) 
						try {
							qc.insert(i, row[i], rowCount);
						}catch (ArrayIndexOutOfBoundsException e) {
							qc.insert(i, "", rowCount);
						}
					if (rowCount % 5000 == 0)
						System.out.println(rowCount + " rows inserted");
				}
				qc.finalize();
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		
			System.out.println(rowCount + " keys inserted");
			stopwatch.printElapsedtimeAndReset();
			FileOutputStream file = null;
			ObjectOutputStream out = null;
			try {
				file = new FileOutputStream(qcTableName);
				out = new ObjectOutputStream(file);
				out.writeObject(qc);
				System.out.println("Serialized...");
				stopwatch.printElapsedtimeAndReset();
			}
			catch(OutOfMemoryError o) {
				System.out.println("Failed to serialize qc data");
				o.printStackTrace();
				if (f.exists()) 
					f.delete();				
			}
			finally {
				out.close();
				file.close();				
			}
		}
		
		System.out.println(Arrays.deepToString(qc.getRow(10)));		
		stopwatch.printElapsedtimeInMillisAndReset();
							
		/*int[] a = qc.getRows(2, new String[] {"cher", "goddess"});
		String[][] b = qc.getRows(a);
		for (String[] c : b)
			System.out.println(Arrays.deepToString(c));
		stopwatch.printElapsedtimeInMillisAndReset();*/
		
		int[] a = qc.getRows(13, new String[] {"jaspper bedlinen", "lorne bedlinen"});
		String[][] b = qc.getRows(a);
		for (String[] c : b)
			System.out.println(Arrays.deepToString(c));
		stopwatch.printElapsedtimeInMillisAndReset();
		
	}
}
