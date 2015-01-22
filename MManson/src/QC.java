import java.io.IOException;
import java.util.Arrays;

import helpers.CharArrayUtils;
import helpers.DataLoader;
import helpers.Stopwatch;


public class QC implements Runnable {
	
	private Table table;
	private DataLoader dl;
	private String[] cols;
	
	public QC(Table table, DataLoader dl, String[] cols) {
		this.table = table;
		this.dl = dl;
		this.cols = cols;
	}

	@Override
	public void run() {
		int rowCount = 0;
		try {
			while(dl.next()){ 		
				rowCount++;
				String[] row = dl.getCurrentRow();
				for (int i = 0; i < cols.length; i++) 
					try {
						table.insert(i, row[i], rowCount);
					}catch (ArrayIndexOutOfBoundsException e) {
						table.insert(i, "", rowCount);
					}
				if (rowCount % 100000 == 0)
					System.out.println(rowCount + " rows inserted");
			}
			table.finalize();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		System.out.println(rowCount + " keys inserted");
	}	

	public static void main(String[] args) throws ClassNotFoundException, IOException {
    	Table[] tables = new Table[2];
		Stopwatch stopwatch = new Stopwatch();
		String[] cols = new String[]{"ITEM","ITEM_PARENT","PRIMARY_EAN","ITEM_NAME","BRAND","PRODUCT_TYPE","COLOUR",
				"COLOUR_CODE","SIZE1","SIZE1_CODE","SECONDARY_SIZE","SECONDARY_SIZE_CODE","DIMENSION","PRICE","HAZCHEM",
				"SUPPLIER","SUPPLIER_COLOR","INNER_PACK_SIZE","EA_LENGTH","EA_WIDTH","EA_HEIGHT","EA_VOLUME","EA_WEIGHT",
				"RETURNABLE_IND","INSTRUCTION_REQ_IND","RESTRICTED_AGE","ITEM_TYPE"};
		//qc = new QC(new String[]{"tpc", "category_code", "brand", "product_type", "colour", "size1"}, dl.numOfRows());
		/*qc = new QC(new String[]{"ITEM", "TPC", "CATEGORY_CODE", "CLASS_GROUP", "CLASS", "SUBCLASS", "BRAND", "COLOUR_IND",
				"SIZE1_IND", "SIZE2_IND", "ONLINE_IND", "STATUS", "STATUS_DESC", "ITEM_NAME", "ITEM_SHORT_DESC",
				"ITEM_LONG_DESC", "MIN_PRICE", "MAX_PRICE", "IMAGE_ADDR"}, dl.numOfRows(), 0);	*/
		
		//"ITEM","ITEM_PARENT","PRIMARY_EAN","L2_ITEM_NAME","L2_BRAND","L2_PRODUCT_TYPE","L2_COLOUR","L2_SIZE1","L1_ITEM_NAME","L1_ITEM_LONG_DESC"


		DataLoader dl = null;/*DataLoader.getInstance("..\\..\\mmsil2.csv", 500000);	
		stopwatch.printElapsedtimeAndReset();
		tables[0] = new Table(cols, dl.numOfRows(), 0);

		//(new Thread(new QC(table, dl, cols))).start();
		
		int rowCount = 0;
		try {
			while(dl.next()){ 		
				rowCount++;
				String[] row = dl.getCurrentRow();
				for (int i = 0; i < tables[0].numOfCols(); i++) 
					try {
						tables[0].insert(i, CharArrayUtils.unqoute(row[i]), rowCount);
					}catch (ArrayIndexOutOfBoundsException e) {
						tables[0].insert(i, "", rowCount);
					}
				if (rowCount % 100000 == 0)
					System.out.println(rowCount + " rows inserted");
			}
			tables[0].finalize();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		System.out.println(rowCount + " keys inserted");

		
		stopwatch.printElapsedtimeAndReset();*/
		

		dl = DataLoader.getInstance("..\\..\\tbc.csv");	
		stopwatch.printElapsedtimeAndReset();
		tables[1] = new Table((new String[]{"ITEM","ITEM_PARENT","PRIMARY_EAN", "TBC"}), dl.numOfRows(), 0);

		//(new Thread(new QC(table, dl, cols))).start();
		
		int rowCount = 0;
		try {
			while(dl.next()){ 		
				rowCount++;
				String[] row = dl.getCurrentRow();
				//for (int i = 0; i < tables[1].numOfCols(); i++) 
					try {
						tables[1].insert(0, CharArrayUtils.unqoute(row[0]), rowCount);
					}catch (ArrayIndexOutOfBoundsException e) {
						tables[1].insert(0, "", rowCount);
					}
					try {
						tables[1].insert(1, CharArrayUtils.unqoute(row[1]), rowCount);
					}catch (ArrayIndexOutOfBoundsException e) {
						tables[1].insert(1, "", rowCount);
					}
					try {
						tables[1].insert(2, CharArrayUtils.unqoute(row[2]), rowCount);
					}catch (ArrayIndexOutOfBoundsException e) {
						tables[1].insert(2, "", rowCount);
					}
					try {
						tables[1].insert(3, CharArrayUtils.unqoute(row[3])+" "+
								CharArrayUtils.unqoute(row[4])+" "+
								CharArrayUtils.unqoute(row[5])+" "+
								CharArrayUtils.unqoute(row[6])+" "+
								CharArrayUtils.unqoute(row[7])+" "+
								CharArrayUtils.unqoute(row[8])+" "+
								CharArrayUtils.unqoute(row[9])+" "
								, rowCount);
					}catch (ArrayIndexOutOfBoundsException e) {
						tables[1].insert(3, "", rowCount);
					}
				if (rowCount % 100000 == 0)
					System.out.println(rowCount + " rows inserted");
			}
			tables[1].finalize();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		System.out.println(rowCount + " keys inserted");
		
		stopwatch.printElapsedtimeAndReset();	
		
		int[] a = tables[1].getRowNumbersContainingKeys(3, "sandler brown boot");
		stopwatch.printElapsedtimeAndReset();	
		
		String[][] ss = tables[1].getRows(a);
		stopwatch.printElapsedtimeAndReset();	
		
		int h = 0;
		for (String[] c : ss)
			System.out.println(++h + ") " + Arrays.deepToString(c));

	}


}
