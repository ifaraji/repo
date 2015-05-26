package com.tabops;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.Table;

//TODO will initiate a request, will perform all the necessary filtering and joins and ..., finally, when requested, returns back the dataset and dismisses the process
public class Process {
	String[][] resultSet;
	Table[] tables;
	HashMap<String, Integer> tablesNameIdMap; 
	
	SQLNode root;
	SQLNode currentNode;
	
	Stack<SQLNode> curStack = new Stack<SQLNode>();
	
	String[] keywords = new String[]{"ds", "table", "cols", "where", "or-block", "and-block", "join", "on"};
	
	StreamTokenizer st;
			
	public Process(InputStream in) throws IOException{
		st = new StreamTokenizer(new InputStreamReader(in));
		st.whitespaceChars(' ',' ');
		st.whitespaceChars('\t','\t');
		st.whitespaceChars('\n','\n');
		st.whitespaceChars('\r','\r');
		st.wordChars('_', '_');
		st.wordChars('-', '-');
		st.ordinaryChar('{');
		st.ordinaryChar('}');
		st.ordinaryChar('"');
		st.ordinaryChar('\'');
		
		Arrays.sort(keywords);
		
		root = new SQLNode();
		currentNode = root;
		
		parse();
	}
	
	private void parse() throws IOException {
		String token = "";
		
		while (st.nextToken() != StreamTokenizer.TT_EOF){
			token = "";
			
			if (st.ttype == StreamTokenizer.TT_WORD)
				token = st.sval; 
			else if (st.ttype == StreamTokenizer.TT_NUMBER)
				token =  String.valueOf(st.nval);
			else if (st.ttype == '{') {
				curStack.push(currentNode);
			}
			else if (st.ttype == '}'){
				curStack.pop();
				if (!curStack.isEmpty())
					currentNode = curStack.peek();
			}
			
			if (token.length() > 0){						
				//System.out.println(token);
				
				if	(Arrays.binarySearch(keywords, 0, keywords.length, token) >= 0) {	
					if (!curStack.isEmpty())
						currentNode = curStack.peek();

					if (token.equals("ds")) 
						currentNode = currentNode.addDs();						
					else if (token.equals("table"))
						currentNode = currentNode.addTable();
					else if (token.equals("cols"))
						currentNode = currentNode.addCols();
					else if (token.equals("where"))
						currentNode = currentNode.addWhere();
					else if (token.equals("or-block"))
						currentNode = currentNode.addOr();
					else if (token.equals("and-block"))
						currentNode = currentNode.addAnd();
					else if (token.equals("join"))
						currentNode = currentNode.addJoin();
					else if (token.equals("on"))
						currentNode = currentNode.addOn();
				} else {//keywords
					currentNode.addData(token);
				}//keywords
			}//(token.length() > 0)
		}//while			
	}
	
	public void execute(){
		SQLNode currentDs = root.ds;
		int tableIndex = tablesNameIdMap.get(currentDs.table.getTableName());
		Map<String, Integer> colsNameIdMap = tables[tableIndex].getColsNameIdMap();
		//TODO form output_cols_int[]
		//TODO iterate where clause
		//TODO 		foreach where clause int[] getRowNumbers(int columnIndex, String key) or int[] getRowNumbers(int columnIndex, String[] key) 
		//TODO			or   getRowNumbersContainingKeys  or   getRowNumbersContaining
		//TODO 			performing necessary ORs and ANDs
		//TODO get table's output cols based on the where's result     
	}
	
	public void run(){
	}
	
	//TODO ithTableOperation(int i)
	//public returnType ithTableOperation(int i) {
	//	return tables[i].operation
	//}
	public void someTableOperation(int tableIndex) {
		
	}
	
	public static void main(String[] args) throws IOException {
		String exampleString  = 
			"\"ds\":{\n" +
			"  \"table\":\"table1\",\n" + 
			"  \"cols\":[\"col1\", \"col2\", \"col3\", \"col4\", \"col5\"],\n" + 
			"  \"where\":{\n" + 
			"    \"or-block\":{\n" + 
			"      \"and-block\":{\"col1\":\"val-1\",\"col2\":\"val-2\"},\n" + 
			"      \"col3\":\"val-3\"\n" + 
			"    }\n" + 
			"  },\n" + 
			"  \"join\":{\n" + 
			"    \"on\":[\"1\", \"2\"],\n" + 
			"    \"ds\":{\n" + 
			"      \"table\":\"table2\",\n" + 
			"      \"cols\":[\"col1\", \"col2\", \"col3\"],\n" + 
			"      \"where\":{\n" + 
			"        \"and-block\":{\"col1\":\"val-1\"}\n" + 
			"      },\n" + 
			"      \"join\":{\n" + 
			"        \"on\":[\"1\"],\n" + 
			"        \"ds\":{\n" + 
			"          \"table\":\"table4\",\n" + 
			"          \"cols\":[\"tpc\"],\n" + 
			"          \"where\":{}\n" + 
			"        }\n" + 
			"      }\n" + 
			"    }\n" + 
			"  },\n" + 
			"  \"join\":{\n" + 
			"    \"on\":[\"3\"],\n" + 
			"    \"ds\":{\n" + 
			"      \"table\":\"table3\",\n" + 
			"      \"cols\":[\"col1\", \"col2\", \"col3\"],\n" + 
			"      \"where\":{}\n" + 
			"    }\n" + 
			"  }\n" + 
			"}";


		InputStream stream = new ByteArrayInputStream(exampleString.getBytes(StandardCharsets.UTF_8));
		Process p = new Process(stream);
	}
}
