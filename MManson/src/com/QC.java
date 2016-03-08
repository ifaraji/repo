package com;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.xml.sax.SAXException;

import com.helpers.DBConnection;
import com.helpers.DataLoader;
import com.helpers.QCDotXMLReader;
import com.helpers.Stopwatch;
import com.helpers.TableObj;
import com.tabops.MergeJoin;

import javax.xml.parsers.ParserConfigurationException;

//TODO starts a stand-alone rest server
public class QC {
	
	//TODO Process[] processes

	Table[] tables;
	int[] finishedTabs;
	
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
	
	private String roll() throws ClassNotFoundException, IOException, ParserConfigurationException, SAXException, SQLException {
		DataLoader dl = null;
		
		//TODO load estimated number of rows from xml
		File xmlDataStructure = new File("..\\..\\qc.xml");
		
		ArrayList<TableObj> tableObjs = QCDotXMLReader.getTableObjs(xmlDataStructure);
		
		tables = new Table[tableObjs.size()];
		finishedTabs = new int[tableObjs.size()];
		
		int index = 0;
		for (TableObj tableObj : tableObjs) {
			//TODO DBConnection.getMRPSConnection()
			//dl = DataLoader.getInstance(tableObj.query, DBConnection.getMRPSConnection(), 6200000);
			
			dl = DataLoader.getInstance("..\\..\\keywords.csv", 6200000);
			
			tables[index] = new Table(tableObj.columns, dl.numOfRows(), index);
			tables[index].load(dl, this);
			
			index++;
		}
		
		/*dl = DataLoader.getInstance("..\\..\\tbc.csv");	
		tables[1] = new Table((new String[]{"ITEM","ITEM_PARENT","PRIMARY_EAN", "TBC"}), dl.numOfRows(), 0);
		tables[1].setId(1);
		tables[1].load(dl, 3, this);		
		
		dl = DataLoader.getInstance("..\\..\\mmsil2.csv");	
		tables[0] = new Table(new String[]{"ITEM","ITEM_PARENT","PRIMARY_EAN","ITEM_NAME","BRAND","PRODUCT_TYPE","COLOUR","COLOUR_CODE","SIZE1","SIZE1_CODE","SECONDARY_SIZE","SECONDARY_SIZE_CODE","DIMENSION","PRICE","HAZCHEM","SUPPLIER","SUPPLIER_COLOR","INNER_PACK_SIZE","EA_LENGTH","EA_WIDTH","EA_HEIGHT","EA_VOLUME","EA_WEIGHT","RETURNABLE_IND","INSTRUCTION_REQ_IND","RESTRICTED_AGE","ITEM_TYPE"}, dl.numOfRows(), 0);
		tables[0].setId(0); 
		tables[0].load(dl, -1, this);*/
		
		while(!allTabsFinished());
		
		return "done";
	}
	
	//TODO getTableByName for use by Process
	//TODO getTableById for use by Process
	
	public static void main(String[] args) throws Exception {
		
		QC qc = new QC();
		System.out.println(qc.roll());
		
		Stopwatch stopwatch = new Stopwatch();
		
		Scanner scanner = new Scanner(System.in);
		boolean bc = true;
		while (bc) {			
			String in = scanner.next();
			if (in.equals("end"))
				bc = false;
			else {
				BufferedReader br = null;
				try
				{
					br = new BufferedReader(new FileReader("..//..//inputQ.txt"));
					String line = br.readLine();
					
					stopwatch.reset();
					
					String[] keys = line.split(" ");
					
					//merge join ******************
					int[] rownums = null;
					String[][] resultSet = null;
					String[][] supResultSet = null;
					for(int i = 1; i < keys.length; i++) {
						String key = keys[i];
						rownums = qc.tables[0].getRowNumbers(1, key, Integer.valueOf(keys[0]));
						stopwatch.printElapsedtimeInMillis("getRowNumbers");
						supResultSet = qc.tables[0].getRowsPath(rownums);
						stopwatch.printElapsedtimeInMillis("getRowsPath");
						resultSet = MergeJoin.apply(resultSet, 0, supResultSet, 0);
						stopwatch.printElapsedtimeInMillis("MergeJoin");
					}
					
					for (int i = 0; i < resultSet.length; i++) {
						resultSet[i][0] = qc.tables[0].getColumnValue(0, resultSet[i][0]);
						resultSet[i][1] = qc.tables[0].getColumnValue(1, resultSet[i][1]);
						resultSet[i][2] = qc.tables[0].getColumnValue(0, resultSet[i][2]);
						resultSet[i][3] = qc.tables[0].getColumnValue(1, resultSet[i][3]);
						resultSet[i][4] = qc.tables[0].getColumnValue(0, resultSet[i][4]);
						resultSet[i][5] = qc.tables[0].getColumnValue(1, resultSet[i][5]);
					}
					
					stopwatch.printElapsedtimeInMillisAndReset();
					
					int j = 0;
					for (String[] c : resultSet) {
						System.out.print("<<" + c[0] + ">> ");
						System.out.println(++j + ") " + Arrays.deepToString(c));						
					}
					
				}
				finally {
					br.close();
				}

			}
		}
		scanner.close();

	}


}
