package com;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import org.xml.sax.SAXException;

import com.helpers.DBConnection;
import com.helpers.DataLoader;
import com.helpers.QCDotXMLReader;
import com.helpers.TableObj;

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
		
		File xmlDataStructure = new File("..\\..\\qc.xml");
		
		ArrayList<TableObj> tableObjs = QCDotXMLReader.getTableObjs(xmlDataStructure);
		
		tables = new Table[tableObjs.size()];
		finishedTabs = new int[tableObjs.size()];
		
		int index = 0;
		for (TableObj tableObj : tableObjs) {
			
			dl = DataLoader.getInstance(tableObj.query,DBConnection.getMRPSConnection());
					
			tables[index] = new Table(tableObj.columns, dl.numOfRows());
			tables[index].setId(index);
			tables[index].load(dl, -1, this);
			
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
	
	public static void main(String[] args) {
		
		QC qc = new QC();
		try {
			System.out.println(qc.roll());
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}


}
