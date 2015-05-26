package com.tabops;

public class SQLNode {
	
	public SQLNode ds, table, cols, where, or, and, on;
	public SQLNode[] joins = new SQLNode[10];
	int joinsIndex = 0;

	String[] data = new String[10];
	int dataIndex = 0;

	String nodeType;

	public void addData(String item) {
		data[dataIndex++] = item;
	}

	public SQLNode addDs() {
		ds = new SQLNode();
		ds.nodeType = "DS";
		return ds;
	}

	public SQLNode addTable() {
		table = new SQLNode();
		table.nodeType = "TABLE";
		return table;
	}

	public SQLNode addCols() {
		cols = new SQLNode();
		cols.nodeType = "COLS";
		return cols;
	}

	public SQLNode addWhere() {
		where = new SQLNode();
		where.nodeType = "WHERE";
		return where;
	}

	public SQLNode addOr() {
		or = new SQLNode();
		or.nodeType = "OR";
		return or;
	}

	public SQLNode addAnd() {
		and = new SQLNode();
		and.nodeType = "AND";
		return and;
	}

	public SQLNode addJoin() {
		joins[joinsIndex] = new SQLNode();
		joins[joinsIndex].nodeType = "JOIN";
		joinsIndex++;
		return joins[joinsIndex - 1];
	}

	public SQLNode addOn() {
		on = new SQLNode();
		on.nodeType = "ON";
		return on;
	}
	
	public String getTableName(){
		if (nodeType.equals("TABLE"))
			return data[0];
		else
			return "";
	}

}
