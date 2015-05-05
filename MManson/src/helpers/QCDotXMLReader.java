package helpers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class QCDotXMLReader {

	private static final String XML_TABLE_NODE = "table";
	private static final String XML_TABLE_NAME_NODE = "name";
	private static final String XML_TABLE_QUERY_NODE = "query";
	private static final String XML_TABLE_COLUMNS_NODE = "columns";
	private static final String XML_TABLE_COLUMN_NODE = "column";
	
	public static ArrayList<TableObj> getTableObjs(File f) throws SAXException, IOException, ParserConfigurationException{
		ArrayList<TableObj> tableObjs = new ArrayList<TableObj>();  
		
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(f);	

		Node tablesNode = doc.getElementsByTagName("tables").item(0); 		

		Node table = tablesNode.getFirstChild();
		while (table != null && table.getNodeType() != Node.ELEMENT_NODE && !table.getNodeName().toLowerCase().equals(XML_TABLE_NODE))
			table = table.getNextSibling();
		while (table != null) {			
			TableObj tableObj = new TableObj();		
			
			Node n = null;
			for (int i = 0; i < table.getChildNodes().getLength(); i++)
				if (table.getChildNodes().item(i).getNodeName().toLowerCase().equals(XML_TABLE_NAME_NODE)) {
					n = table.getChildNodes().item(i);
					break;
				}
			if (n != null)
				tableObj.name = n.getTextContent();
			
			Node q = null;
			for (int i = 0; i < table.getChildNodes().getLength(); i++)
				if (table.getChildNodes().item(i).getNodeName().toLowerCase().equals(XML_TABLE_QUERY_NODE)) {
					q = table.getChildNodes().item(i);
					break;
				}
			if (q != null)
				tableObj.query = q.getTextContent();
			
			Node c = null;
			for (int i = 0; i < table.getChildNodes().getLength(); i++)
				if (table.getChildNodes().item(i).getNodeName().toLowerCase().equals(XML_TABLE_COLUMNS_NODE)) {
					c = table.getChildNodes().item(i);
					break;
				}
			if (c != null) {
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < c.getChildNodes().getLength(); i++)
					if (c.getChildNodes().item(i).getNodeName().toLowerCase().equals(XML_TABLE_COLUMN_NODE)) {						
						sb.append(c.getChildNodes().item(i).getTextContent()) ;
						sb.append(",");
					}				
				tableObj.columns = sb.substring(0, sb.length() - 1).split(",");
			}
			
			tableObjs.add(tableObj);
			
			table = table.getNextSibling();
			while (table != null && table.getNodeType() != Node.ELEMENT_NODE && !table.getNodeName().toLowerCase().equals(XML_TABLE_NODE))
				table = table.getNextSibling();
		}
		
		return tableObjs;
	}
	
	public static void main(String[] args) throws SAXException, IOException, ParserConfigurationException {
		File xmlDataStructure = new File("..\\..\\qc.xml");
		
		ArrayList<TableObj> tableObjs = QCDotXMLReader.getTableObjs(xmlDataStructure);
		for (TableObj tableObj : tableObjs)
			System.out.println(tableObj);
	}
}
