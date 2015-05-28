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
import com.helpers.IntArrayUtils;

public class Process {
	String[][] resultSet;
	Table[] tables;
	HashMap<String, Integer> tablesNameIdMap; 
	
	SQLNode root;
	SQLNode currentNode;
	
	Stack<SQLNode> curStack = new Stack<SQLNode>();
	
	StreamTokenizer st;
	
	private final String KEYWORD_DS = "ds";
	private final String KEYWORD_TABLE = "table";
	private final String KEYWORD_COLS = "cols";
	private final String KEYWORD_WHERE = "where";
	private final String KEYWORD_OR = "or-block";
	private final String KEYWORD_AND = "and-block";
	private final String KEYWORD_JOIN = "join";
	private final String KEYWORD_ON = "on";
			
	String[] keywords = new String[]{KEYWORD_DS, KEYWORD_TABLE, KEYWORD_COLS, KEYWORD_WHERE, 
									 KEYWORD_OR, KEYWORD_AND, KEYWORD_JOIN, KEYWORD_ON};
	
	ProcessDelegate delegate;
	
	public Process(InputStream in) throws IOException{
		st = new StreamTokenizer(new InputStreamReader(in));
		st.whitespaceChars(' ',' ');
		st.whitespaceChars('\t','\t');
		st.whitespaceChars('\n','\n');
		st.whitespaceChars('\r','\r');
		st.wordChars('_', '_');
		st.wordChars('-', '-');
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

					if (token.equals(KEYWORD_DS)) 
						currentNode = currentNode.addDs();						
					else if (token.equals(KEYWORD_TABLE))
						currentNode = currentNode.addTable();
					else if (token.equals(KEYWORD_COLS))
						currentNode = currentNode.addCols();
					else if (token.equals(KEYWORD_WHERE))
						currentNode = currentNode.addWhere();
					else if (token.equals(KEYWORD_OR))
						currentNode = currentNode.addOr();
					else if (token.equals(KEYWORD_AND))
						currentNode = currentNode.addAnd();
					else if (token.equals(KEYWORD_JOIN))
						currentNode = currentNode.addJoin();
					else if (token.equals(KEYWORD_ON))
						currentNode = currentNode.addOn();
				} else {//keywords
					currentNode.addData(token);
				}//keywords
			}//(token.length() > 0)
		}//while			
	}
	
	public void execute(){
		resultSet = execute(root.ds);	
		delegate.handle(resultSet);
	}
	
	private String[][] execute(SQLNode ds){
		int tableIndex = tablesNameIdMap.get(ds.table.getTableName());
		Map<String, Integer> colsNameIdMap = tables[tableIndex].getColsNameIdMap();
		
		int[] outputColsIndexes = new int[ds.cols.dataIndex];
		for (int i = 0; i < outputColsIndexes.length; i++)
			outputColsIndexes[i] = colsNameIdMap.get(ds.cols.data[i]);
		
		int[] rows = processWhere(ds.where, tableIndex);				
		
		String[][] dsResultSet = tables[tableIndex].getRows(rows);
		
		String[][] joinResultSet = null;
		int joinsCount = root.ds.joinsIndex;
		while(joinsCount > 0){
			joinResultSet = execute(ds.joins[--joinsCount]);
			dsResultSet = MergeJoin.apply(dsResultSet, Integer.valueOf(ds.joins[joinsCount].on.data[0]), 
					joinResultSet, Integer.valueOf(ds.joins[joinsCount].on.data[1]));
		}
		
		return dsResultSet;
	}
	
	//TODO utilize getRowNumbers(int columnIndex, String[] key)
	//TODO utilize getRowNumbersContainingKeys
	//TODO utilize getRowNumbersContaining
	private int[] processWhere(SQLNode where, int tableIndex){
		int[] rows = null;
		if (where.or != null)
			return processOrAnd(where.or, tableIndex);
		else if (where.and != null)
			return processOrAnd(where.and, tableIndex);
		else {
			int i = 0;
			int j = i + 1;		
			if (where.dataIndex > 0) {
				Map<String, Integer> colsNameIdMap = tables[tableIndex].getColsNameIdMap();
				int colIndex = colsNameIdMap.get(where.data[i]);
				rows = tables[tableIndex].getRowNumbers(colIndex, where.data[j]);
			} else {
				rows = tables[tableIndex].getRowNumbers();
			}			
			return rows; 
		}
	}
	
	private int[] processOrAnd(SQLNode orAnd, int tableIndex){
		int[] rowsInnerOr = null;
		int[] rowsInnerAnd = null;
		int[] rowsInnerOrAnd = null;
		int[] rows = null;
		if (orAnd.or != null)
			rowsInnerOr = processOrAnd(orAnd.or, tableIndex);
		if (orAnd.and != null)
			rowsInnerAnd = processOrAnd(orAnd.and, tableIndex);
		
		if (!IntArrayUtils.IsEmpty(rowsInnerOr) && !IntArrayUtils.IsEmpty(rowsInnerAnd))
			rowsInnerOrAnd = tables[tableIndex].or(rowsInnerOr, rowsInnerAnd);
		
		int i = 0;
		int j = i + 1;		
		if (orAnd.dataIndex > 0) {
			while(i < orAnd.dataIndex) {
				Map<String, Integer> colsNameIdMap = tables[tableIndex].getColsNameIdMap();
				int colIndex = colsNameIdMap.get(orAnd.data[i]);
				int[] tempRows = tables[tableIndex].getRowNumbers(colIndex, orAnd.data[j]);
				rows = tables[tableIndex].or(tempRows, rows);
				i += 2;
				j = i + 1;
			}		
		}
		
		if (!IntArrayUtils.IsEmpty(rows) && !IntArrayUtils.IsEmpty(rowsInnerOrAnd))
			rows = tables[tableIndex].or(rows, rowsInnerOrAnd);
		return rows; 
	}

	
	public void run(){
		execute();
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
			"    \"on\":[\"1*1\", \"2*2\"],\n" + 
			"    \"ds\":{\n" + 
			"      \"table\":\"table2\",\n" + 
			"      \"cols\":[\"col1\", \"col2\", \"col3\"],\n" + 
			"      \"where\":{\n" + 
			"        \"and-block\":{\"col1\":\"val-1\"}\n" + 
			"      },\n" + 
			"      \"join\":{\n" + 
			"        \"on\":[\"3*1\"],\n" + 
			"        \"ds\":{\n" + 
			"          \"table\":\"table4\",\n" + 
			"          \"cols\":[\"tpc\"],\n" + 
			"          \"where\":{}\n" + 
			"        }\n" + 
			"      }\n" + 
			"    }\n" + 
			"  },\n" + 
			"  \"join\":{\n" + 
			"    \"on\":[\"3*3\"],\n" + 
			"    \"ds\":{\n" + 
			"      \"table\":\"table3\",\n" + 
			"      \"cols\":[\"col1\", \"col2\", \"col3\"],\n" + 
			"      \"where\":{}\n" + 
			"    }\n" + 
			"  }\n" + 
			"}";


		InputStream stream = new ByteArrayInputStream(exampleString.getBytes(StandardCharsets.UTF_8));
		Process p = new Process(stream);
		p.execute();
	}
}
