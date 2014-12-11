import helpers.DataLoader;
import helpers.Stopwatch;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Scanner;

//TODO sorted result set (requires the trie returns row nums based on its key sorted)
//TODO master field search :) (wanna cater for a search scenario like google, just a text box and no dropdowns, full search criteria is entered in the text box like 'black leather pump'; all related fields should be searched)
public class QC implements Serializable{

	private static final long serialVersionUID = 1L;

    public static class Column implements Serializable{
		private static final long serialVersionUID = 1L;
		private String name;
        private IdeenTrieC col;
        //private Column next; <LL impl>
    }

    //private Column first;    // beginning of bag <LL impl>
    private int colsCount; 		     //number of columns
    
    private Column[] columns;
       
    public QC(String[] cols, int numOfRows){   
    	colsCount = cols.length;
    	columns = new Column[colsCount];
    	
    	for(int i = 0; i < colsCount; i++){
    		columns[i] = new Column();
    		columns[i].name = cols[i];
    		columns[i].col = new IdeenTrieC(numOfRows, false);
    	}
    }
    
    public QC(String[] cols, int numOfRows, int uniqueColumnIndex){   
    	colsCount = cols.length;
    	columns = new Column[colsCount];
    	
    	for(int i = 0; i < colsCount; i++){
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
    	for(int i = 0; i < colsCount; i++){
    		columns[i].col.finalize();
    	}
    }
    
    public int numOfCols() {
    	return colsCount;
    }
    
    public String getColumnName(int columnIndex) {
    	return columns[columnIndex].name;
    }
    
    public Column[] getColumns() {
    	return columns;
    }
      
    public String[] getRow(int rowNum){
    	String[] row = new String[colsCount]; 
    	for(int i = 0; i < colsCount; i++)
    		row[i] = columns[i].col.getRowValue(rowNum);
    	return row;
    }
    
    //TODO should be overloaded to produce JSON,XML and streaming output
    //TODO should be cater for pagination
    public String[][] getRows(int[] rows){
    	String[][] rowSet = new String[rows.length][colsCount];    	
    	for(int i = 0; i < rows.length; i++) {
    		rowSet[i] = getRow(rows[i]);
    	}
    	return rowSet;
    } 
    
    //TODO extract keywords from the key (splitting by space maybe) and perform an "AND" search (i.e. search for "brown boot" and "boot brown" should return the same result sets)
    //TODO Just a note that the above will require a keyContains getRows method to be implemented by the col
    public int[] getRowNumbers(int columnIndex, String key){    	
    	int[] a = columns[columnIndex].col.getRows(key);
    	return a;
    }
    
    public int[] getRowNumbers(int columnIndex, String[] key){    	
    	int[] a = columns[columnIndex].col.getRows(key);
    	return a;
    }        
    
    //TODO ensure the best algo will be impled
    public int[] and(int[] rowNumbers1, int[] rowNumbers2){
    	if ( (rowNumbers1 == null || rowNumbers1.length == 0) || (rowNumbers2 == null || rowNumbers2.length == 0) )
    		return new int[0];
    	
    	int[] tResult = new int[Math.max(rowNumbers1.length, rowNumbers2.length)];
    	int i = 0;
    	int j = 0;
    	int k = 0;
    	
    	while (i < rowNumbers1.length && j < rowNumbers2.length) {
	    	while (i < rowNumbers1.length && rowNumbers1[i] < rowNumbers2[j])	i++;
	    	
	    	while (j < rowNumbers2.length && i < rowNumbers1.length && rowNumbers1[i] > rowNumbers2[j])	j++;
	    	
	    	while (i < rowNumbers1.length && j < rowNumbers2.length && rowNumbers1[i] == rowNumbers2[j]){
	    		tResult[k] = rowNumbers1[i]; 
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
    
  //TODO ensure the best algo will be impled
    public int[] or(int[] rowNumbers1, int[] rowNumbers2){
    	if ( (rowNumbers1 == null || rowNumbers1.length == 0) && (rowNumbers2 != null && rowNumbers2.length > 0) )
    		return rowNumbers2;
    	if ( (rowNumbers2 == null || rowNumbers2.length == 0) && (rowNumbers1 != null && rowNumbers1.length > 0) )
    		return rowNumbers1;
    	
    	int[] result = new int[rowNumbers1.length + rowNumbers2.length];
    	
    	int i = 0;
    	int j = 0;
    	int k = 0;    	
    	
    	while(k < result.length) {
	    	while(i < rowNumbers1.length && (j == rowNumbers2.length || rowNumbers1[i]<rowNumbers2[j])){
	    		result[k] = rowNumbers1[i];
	    		i++;
	    		k++;
	    	}
	    	while(j < rowNumbers2.length && (i == rowNumbers1.length || rowNumbers1[i]>rowNumbers2[j])){
	    		result[k] = rowNumbers2[j];
	    		j++;
	    		k++;
	    	}	    	
    	}
    	    	
    	return result;
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
    	QC qc;
		Stopwatch stopwatch = new Stopwatch();
		
		DataLoader dl = DataLoader.getInstance("..\\..\\mmsil1.csv");	
		//qc = new QC(new String[]{"tpc", "category_code", "brand", "product_type", "colour", "size1"}, dl.numOfRows());
		qc = new QC(new String[]{"ITEM", "TPC", "CATEGORY_CODE", "CLASS_GROUP", "CLASS", "SUBCLASS", "BRAND", "COLOUR_IND",
								"SIZE1_IND", "SIZE2_IND", "ONLINE_IND", "STATUS", "STATUS_DESC", "ITEM_NAME", "ITEM_SHORT_DESC",
								"ITEM_LONG_DESC", "MIN_PRICE", "MAX_PRICE", "IMAGE_ADDR"}, dl.numOfRows(), 0);	
		int rowCount = 0;
		try {
			while(dl.next()){ 		
				rowCount++;
				String[] row = dl.getCurrentRow();
				for (int i = 0; i < qc.colsCount; i++) 
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
		
		int[] a = qc.getRowNumbers(6, "heritage");
		int[] a1 = qc.getRowNumbers(2, "500");
		a = qc.and(a,a1);
		a1 = qc.getRowNumbers(4, "13");
		a = qc.and(a,a1);
		String[][] b = qc.getRows(a);
		int h = 0;
		System.out.println(b.length + " rows found");
		for (String[] c : b)
			System.out.println(++h + ") " + Arrays.deepToString(c));
		stopwatch.printElapsedtimeInMillisAndReset();
		
		Scanner scanner = new Scanner(System.in);
		scanner.next();
		
		System.out.println("done");
	}
}
