import helpers.DataLoader;
import helpers.Stopwatch;

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

    public int[] and(int[] rowset1, int[] rowset2){
    	if ( (rowset1 == null || rowset1.length == 0) || (rowset2 == null || rowset2.length == 0) )
    		return new int[0];
    	
    	int[] tResult = new int[Math.max(rowset1.length, rowset2.length)];
    	int i = 0;
    	int j = 0;
    	int k = 0;
    	
    	while (i < rowset1.length && j < rowset2.length) {
	    	while (i < rowset1.length && rowset1[i] < rowset2[j])	i++;
	    	
	    	while (j < rowset2.length && i < rowset1.length && rowset1[i] > rowset2[j])	j++;
	    	
	    	while (i < rowset1.length && j < rowset2.length && rowset1[i] == rowset2[j]){
	    		tResult[k] = rowset1[i]; 
	    		i++;
	    		j++;
	    		k++;
	    	}
    	}
    	
    	int[] result = new int[k];
    	for (int h = 0; h < k; h++)
    		result[h] = tResult[h];
    	
    	return result;
    }
    
    public int[] or(int[] rowset1, int[] rowset2){
    	if ( (rowset1 == null || rowset1.length == 0) && (rowset2 != null && rowset2.length > 0) )
    		return rowset2;
    	if ( (rowset2 == null || rowset2.length == 0) && (rowset1 != null && rowset1.length > 0) )
    		return rowset1;
    	
    	int[] result = new int[rowset1.length + rowset2.length];
    	
    	int i = 0;
    	int j = 0;
    	int k = 0;    	
    	
    	while(k < result.length) {
	    	while(i < rowset1.length && (j == rowset2.length || rowset1[i]<rowset2[j])){
	    		result[k] = rowset1[i];
	    		i++;
	    		k++;
	    	}
	    	while(j < rowset2.length && (i == rowset1.length || rowset1[i]>rowset2[j])){
	    		result[k] = rowset2[j];
	    		j++;
	    		k++;
	    	}	    	
    	}
    	    	
    	return result;
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
				
		    	/*for(int i = 0; i < qc.M; i++){
			        file = new FileOutputStream("..\\..\\col"+String.valueOf(i)+".dat");
					out = new ObjectOutputStream(file);
					out.writeObject(qc.columns[i]);
					out.close();
					file.close();
		    	}*/
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
		
		System.out.println(Arrays.deepToString(qc.getRow(65348)));		
		stopwatch.printElapsedtimeInMillisAndReset();
							
		/*int[] a = qc.getRows(2, new String[] {"cher", "goddess"});
		String[][] b = qc.getRows(a);
		for (String[] c : b)
			System.out.println(Arrays.deepToString(c));
		stopwatch.printElapsedtimeInMillisAndReset();*/
		
		/*int[] a = qc.getRows(13, new String[] {"jaspper bedlinen", "lorne bedlinen"});
		int[] a1 = qc.getRows(6, "heritage");
		int[] a2 = qc.or(a,a1);		*/
		
		int[] a = qc.getRows(6, "heritage");
		int[] a1 = qc.getRows(2, "500");
		a = qc.and(a,a1);
		a1 = qc.getRows(4, "13");
		a = qc.and(a,a1);
		String[][] b = qc.getRows(a);
		int h = 0;
		System.out.println(b.length + " rows found");
		for (String[] c : b)
			System.out.println(++h + ") " + Arrays.deepToString(c));
		stopwatch.printElapsedtimeInMillisAndReset();
		
	}
}
