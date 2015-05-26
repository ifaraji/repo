package com;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Map;
import java.util.Scanner;

import com.helpers.CharArrayUtils;
import com.helpers.DataLoader;
import com.helpers.IntArrayUtils;
import com.helpers.JSONResultSet;
import com.helpers.Stopwatch;

//TODO master field search :) (wanna cater for a search scenario like google, just a text box and no dropdowns, full search criteria is entered in the text box like 'black leather pump'; all related fields should be searched)
public class Table implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private int tableId;

    public static class Column implements Serializable{
		private static final long serialVersionUID = 1L;
		private String name;
        private IdeenTrieC col;
        //private Column next; <LL impl>
    }
    
    //private Column first;    // beginning of bag <LL impl>
    private int colsCount; 		     //number of columns
    
    private Column[] columns;
    private String[] columnNames;
       
    public Table(String[] cols, int numOfRows){
    	columnNames = cols;
    	colsCount = cols.length;
    	columns = new Column[colsCount];
    	
    	for(int i = 0; i < colsCount; i++){
    		columns[i] = new Column();
    		columns[i].name = cols[i];
    		columns[i].col = new IdeenTrieC(numOfRows, false);
    	}
    }
    
    public Table(String[] cols, int numOfRows, int uniqueColumnIndex){   
    	columnNames = cols;
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
    
    //TODO ...!!!
    public void setId(int id) {
    	tableId = id;
    }
    
    public Table insert(int columnId, String columnValue, int rowIndex) {
    	columns[columnId].col.insert(columnValue, rowIndex);
    	return this;
    }
    
    public void seal() {
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
    
    //TODO should cater for pagination
    public String[][] getRows(int[] rows){
    	String[][] rowSet = new String[rows.length][colsCount];    	
    	for(int i = 0; i < rows.length; i++) {
    		rowSet[i] = getRow(rows[i]);
    	}
    	//TODO sort based on column index
    	return rowSet;
    }
    
    public String getJSONRows(int[] rows){
    	String[][] rowSet = getRows(rows);    	    	
		JSONResultSet j = new JSONResultSet(this.columnNames);		
    	return j.generate(rowSet);
    }
    
    //TODO getXMLRows
    public String getXMLRows(int[] rows){
    	throw new UnsupportedOperationException("Not implemented yet!");
    }
    
    //TODO getStreamRows
    public String getStreamRows(int[] rows){
    	throw new UnsupportedOperationException("Not implemented yet!");
    }    
    
    public int[] getRowNumbers(int columnIndex, String key){    	
    	int[] a = columns[columnIndex].col.getRows(key);
    	return a;
    }
    
    public int[] getRowNumbers(int columnIndex, String[] key){    	
    	int[] a = columns[columnIndex].col.getRows(key);
    	return a;
    }        
    
    public int[] getRowNumbersContaining(int columnIndex, String key){  
    	return columns[columnIndex].col.getRowsContaining(key);
    }
    
    public int[] getRowNumbersContainingKeys(int columnIndex, String key){  
    	//String[] keys = key.split(" ");
    	//int[][] allRows = new int[keys.length][]; 
    	//for(int i = 0; i < keys.length; i++) 
    		//allRows[i] = columns[columnIndex].col.getRowsContaining(keys[i]);
    	//int[] result = allRows[0];
    	//for(int i = 1; i < allRows.length; i++)
    		//result = and(result,allRows[i]);    	
    	//return result;
    	return columns[columnIndex].col.getRowsContaining(key, " ");
    }            
    
    public int[] getRowNumbersContainingPattern(int columnIndex, String regex){  
    	return columns[columnIndex].col.getRowsContainingPattern(regex);
    }            
    
    public int[] and(int[] rowNumbers1, int[] rowNumbers2){
    	return IntArrayUtils.intersection(rowNumbers1, rowNumbers2);
    }
    
    public int[] or(int[] rowNumbers1, int[] rowNumbers2){
    	return IntArrayUtils.union(rowNumbers1, rowNumbers2);
    }
    
    //TODO loads this table. 
    //TODO must be asynchronous
    //TODO tbc reference is not appropriate here, must be handled in QC where query and table structure is being determined
    public void load(DataLoader dl, int tbc, QC qc) {
    	
    	class HelperThread extends Thread {
    		DataLoader dl;
    		int tbc;
    		QC qc;
    		public HelperThread(DataLoader dl, int tbc, QC qc){
    			super();
    			this.dl = dl;
    			this.tbc = tbc;
    			this.qc = qc;
    		}
    	}
    	
    	new HelperThread(dl, tbc, qc) {
    		public void run() {
    			int rowCount = 0;
    			if (this.tbc < 0)
    				this.tbc = colsCount;
    			try {
    				while(this.dl.next()){ 		
    					rowCount++;
    					String[] row = this.dl.getCurrentRow();
    					for (int i = 0; i < this.tbc; i++) 
    						try {
    							insert(i, CharArrayUtils.unqoute(row[i]), rowCount);
    						}catch (ArrayIndexOutOfBoundsException e) {
    							insert(i, "", rowCount);
    						}

    					if (tbc < colsCount) {
    						StringBuilder mergedCols = new StringBuilder("");
    						for (int j = tbc; j < row.length; j++)		
    							mergedCols.append(CharArrayUtils.unqoute(row[j])+" ");
    						insert(tbc,mergedCols.toString(), rowCount);
    					}
    					if (rowCount % 100000 == 0) System.out.println("Tab-"+tableId+": " + rowCount + " rows inserted");
    				}
    				seal();
    			}
    			catch(Exception e) {
    				e.printStackTrace();
    			}
    			qc.tabFinished(tableId);
			}
    	}.start();
    	
		/*int rowCount = 0;
		if (tbc < 0)
			tbc = colsCount;
		try {
			while(dl.next()){ 		
				rowCount++;
				String[] row = dl.getCurrentRow();
				for (int i = 0; i < tbc; i++) 
					try {
						insert(i, CharArrayUtils.unqoute(row[i]), rowCount);
					}catch (ArrayIndexOutOfBoundsException e) {
						insert(i, "", rowCount);
					}

				if (tbc < colsCount) {
					StringBuilder mergedCols = new StringBuilder("");
					for (int j = tbc; j < row.length; j++)		
						mergedCols.append(CharArrayUtils.unqoute(row[j])+" ");
					insert(tbc,mergedCols.toString(), rowCount);
				}
				//if (rowCount % 100000 == 0) System.out.println(rowCount + " rows inserted");
			}
			finalize();
		}
		catch(Exception e) {
			e.printStackTrace();
		}*/
    }
    
    //TODO getColsNameIdMap()
    public Map<String, Integer> getColsNameIdMap(){
    	throw new UnsupportedOperationException("Not implemented yet!");  
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
    	Table qc;
		Stopwatch stopwatch = new Stopwatch();
		
		DataLoader dl = DataLoader.getInstance("..\\..\\mmsil2.csv", 500000);	
		stopwatch.printElapsedtimeAndReset();
		//qc = new QC(new String[]{"tpc", "category_code", "brand", "product_type", "colour", "size1"}, dl.numOfRows());
		qc = new Table(new String[]{"ITEM","ITEM_PARENT","PRIMARY_EAN","ITEM_NAME","BRAND","PRODUCT_TYPE","COLOUR","COLOUR_CODE","SIZE1","SIZE1_CODE","SECONDARY_SIZE","SECONDARY_SIZE_CODE","DIMENSION","PRICE","HAZCHEM","SUPPLIER","SUPPLIER_COLOR","INNER_PACK_SIZE","EA_LENGTH","EA_WIDTH","EA_HEIGHT","EA_VOLUME","EA_WEIGHT","RETURNABLE_IND","INSTRUCTION_REQ_IND","RESTRICTED_AGE","ITEM_TYPE"}, dl.numOfRows(), 0);
		/*qc = new QC(new String[]{"ITEM", "TPC", "CATEGORY_CODE", "CLASS_GROUP", "CLASS", "SUBCLASS", "BRAND", "COLOUR_IND",
								"SIZE1_IND", "SIZE2_IND", "ONLINE_IND", "STATUS", "STATUS_DESC", "ITEM_NAME", "ITEM_SHORT_DESC",
								"ITEM_LONG_DESC", "MIN_PRICE", "MAX_PRICE", "IMAGE_ADDR"}, dl.numOfRows(), 0);	*/		
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
				if (rowCount % 100000 == 0)
					System.out.println(rowCount + " rows inserted");
			}
			qc.seal();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(rowCount + " keys inserted");
		stopwatch.printElapsedtimeAndReset();
		
		//***********************************************************************
		int[] a; 
		//int[] a1;
		//String[][] b;

		/*System.out.println(Arrays.deepToString(qc.getRow(65348)));		
		stopwatch.printElapsedtimeInMillisAndReset();

		System.out.println(Arrays.deepToString(qc.getRow(100)));		
		System.out.println(Arrays.deepToString(qc.getRow(150)));		
		System.out.println(Arrays.deepToString(qc.getRow(200)));		
		System.out.println(Arrays.deepToString(qc.getRow(250)));		
		stopwatch.printElapsedtimeInMillisAndReset();*/
		
		/*a = qc.getRows(2, new String[] {"cher", "goddess"});
		b = qc.getRows(a);
		for (String[] c : b)
			System.out.println(Arrays.deepToString(c));
		stopwatch.printElapsedtimeInMillisAndReset();*/
		
		/*a = qc.getRows(13, new String[] {"jaspper bedlinen", "lorne bedlinen"});
		a1 = qc.getRows(6, "heritage");
		a2 = qc.or(a,a1);		*/
		
		/*a = qc.getRowNumbers(6, "heritage");
		a1 = qc.getRowNumbers(2, "500");
		a = qc.and(a,a1);
		a1 = qc.getRowNumbers(4, "13");
		a = qc.and(a,a1);
		b = qc.getRows(a);
		int h = 0;
		System.out.println(b.length + " rows found");
		for (String[] c : b)
			System.out.println(++h + ") " + Arrays.deepToString(c));
		stopwatch.printElapsedtimeInMillisAndReset();
		
		a = qc.getRowNumbersContainingKeys(13, "brown boot");
		System.out.println(a.length + " rows found");
		stopwatch.printElapsedtimeInMillisAndReset();		
		b = qc.getRows(a);		
		h = 0;
		for (String[] c : b)
			System.out.println(++h + ") " + Arrays.deepToString(c));
		stopwatch.printElapsedtimeInMillisAndReset();

		a = qc.getRowNumbersContainingKeys(13, "brown boot");
		System.out.println(a.length + " rows found");
		stopwatch.printElapsedtimeInMillisAndReset();
		System.out.println(qc.getJSONRows(a));
		stopwatch.printElapsedtimeInMillisAndReset();*/
		
		Scanner scanner = new Scanner(System.in);
		boolean bc = true;
		while (bc) {
			stopwatch.reset();
			/*a = qc.getRowNumbersContaining(3, "strip");
			System.out.println(a.length + " rows found");
			stopwatch.printElapsedtimeInMillisAndReset();
			qc.getJSONRows(a);
			System.out.println("To JSON string...");
			stopwatch.printElapsedtimeInMillisAndReset();*/

			stopwatch.reset();
			a = qc.getRowNumbersContainingKeys(3, "strip blue");
			System.out.println(a.length + " rows found");
			stopwatch.printElapsedtimeInMillisAndReset();			
			qc.getJSONRows(a);//System.out.println(qc.getJSONRows(a));
			System.out.println("To JSON string...");
			stopwatch.printElapsedtimeInMillisAndReset();

			
			a = qc.getRowNumbersContaining(3, "strip");
			int[] b = qc.getRowNumbersContaining(3, "blue");
			a = qc.and(a, b);
			System.out.println(a.length + " rows found");
			stopwatch.printElapsedtimeInMillisAndReset();			
			qc.getJSONRows(a);//System.out.println(qc.getJSONRows(a));
			System.out.println("To JSON string...");
			stopwatch.printElapsedtimeInMillisAndReset();

			
			String in = scanner.next();
			if (in.equals("end"))
				bc = false;
		}
		scanner.close();
		System.out.println("done");
	}
}
