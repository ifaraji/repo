import java.io.IOException;
import java.util.Arrays;

import helpers.DataLoader;
import helpers.Stopwatch;


public class QC {
	Table[] tables = new Table[2];
	int[] finishedTabs = new int[2];
	
	public void tabFinished(int index) {
		finishedTabs[index] = 1;
		System.out.println("Tab " + index + " finished loading");
	}
	
	private boolean allTabsFinished() {
		int sum = 0;
		for (int j = 0; j < finishedTabs.length; j++)
			sum += finishedTabs[j];
		return sum == finishedTabs.length;
	}
	
	private String roll() throws ClassNotFoundException, IOException {
		DataLoader dl = null;		

		dl = DataLoader.getInstance("..\\..\\tbc.csv");	
		tables[1] = new Table((new String[]{"ITEM","ITEM_PARENT","PRIMARY_EAN", "TBC"}), dl.numOfRows(), 0);
		tables[1].setId(1);
		tables[1].load(dl, 3, this);		
		
		dl = DataLoader.getInstance("..\\..\\mmsil2.csv");	
		tables[0] = new Table(new String[]{"ITEM","ITEM_PARENT","PRIMARY_EAN","ITEM_NAME","BRAND","PRODUCT_TYPE","COLOUR","COLOUR_CODE","SIZE1","SIZE1_CODE","SECONDARY_SIZE","SECONDARY_SIZE_CODE","DIMENSION","PRICE","HAZCHEM","SUPPLIER","SUPPLIER_COLOR","INNER_PACK_SIZE","EA_LENGTH","EA_WIDTH","EA_HEIGHT","EA_VOLUME","EA_WEIGHT","RETURNABLE_IND","INSTRUCTION_REQ_IND","RESTRICTED_AGE","ITEM_TYPE"}, dl.numOfRows(), 0);
		tables[0].setId(0); 
		tables[0].load(dl, -1, this);
		
		while(!allTabsFinished());
		
		return "done";
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException {
    	/*Table[] tables = new Table[2];
		Stopwatch stopwatch = new Stopwatch();
		DataLoader dl = null;		

		dl = DataLoader.getInstance("..\\..\\tbc.csv");	
		stopwatch.printElapsedtimeAndReset("DataLoader Load");
		tables[1] = new Table((new String[]{"ITEM","ITEM_PARENT","PRIMARY_EAN", "TBC"}), dl.numOfRows(), 0);
		tables[1].load(dl, 3, null);		
		stopwatch.printElapsedtimeAndReset("Table Load");
		
		dl = DataLoader.getInstance("..\\..\\mmsil2.csv", 500000);	
		stopwatch.printElapsedtimeAndReset("DataLoader Load");
		tables[0] = new Table(new String[]{"ITEM","ITEM_PARENT","PRIMARY_EAN","ITEM_NAME","BRAND","PRODUCT_TYPE","COLOUR","COLOUR_CODE","SIZE1","SIZE1_CODE","SECONDARY_SIZE","SECONDARY_SIZE_CODE","DIMENSION","PRICE","HAZCHEM","SUPPLIER","SUPPLIER_COLOR","INNER_PACK_SIZE","EA_LENGTH","EA_WIDTH","EA_HEIGHT","EA_VOLUME","EA_WEIGHT","RETURNABLE_IND","INSTRUCTION_REQ_IND","RESTRICTED_AGE","ITEM_TYPE"}, dl.numOfRows(), 0);
		tables[0].load(dl, -1, null);*/
		
		QC qc = new QC();
		System.out.println(qc.roll());
		
		/*int[] a = tables[1].getRowNumbersContainingKeys(3, "sandler brown boot");
		stopwatch.printElapsedtimeAndReset("getRowNumbersContainingKeys");	
		
		String[][] ss = tables[1].getRows(a);
		stopwatch.printElapsedtimeAndReset("getRows(a)");	
		
		int h = 0;
		for (String[] c : ss)
			System.out.println(++h + ") " + Arrays.deepToString(c));
		
		System.out.println("********************************");
		
		String s = tables[1].getJSONRows(a);
		stopwatch.printElapsedtimeAndReset("getJSONRows(a)");
		System.out.println(s);*/
	}


}
