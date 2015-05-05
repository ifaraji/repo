package helpers;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class DataLoader implements Serializable
{
	private static final long serialVersionUID = 1L;
	private String[][] rows;
	private int N;
	private int C;
	
	//TODO DataLoader(String query, Connection connection)
	private DataLoader(String query, Connection connection) throws SQLException {
		rows = new String[10000][];
		N = 0;
		C = -1;
		readDatabase(query, connection);
		System.out.println(numOfRows() + " rows loaded");
	}
	
	private DataLoader(String filename) throws IOException {
		rows = new String[10000][];
		N = 0;
		C = -1;
		readFile(filename);
		System.out.println(numOfRows() + " rows loaded");
	}
	
	private DataLoader(String filename, int maxRows) throws IOException {
		rows = new String[10000][];
		N = 0;
		C = -1;
		readFile(filename, maxRows);
		System.out.println(numOfRows() + " rows loaded");
	}
	
	private void readDatabase(String query, Connection connection) throws SQLException {
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery(query);
		int resultSetColCount = resultSet.getMetaData().getColumnCount();

		int i = 0;
		while (resultSet.next()) {
			rows[i] = new String[resultSetColCount];			
			N++;
		    for (int j = 1; j <= resultSetColCount; j++)
		    	rows[i][j-1] = resultSet.getString(j);
        	i++;
        	if (i == rows.length)
        		expandRows();
		}
		resultSet.close();
	}	
	
	private void readFile(String filename) throws IOException {
		BufferedReader br = null;
		int i = 0;
		try
		{
			br = new BufferedReader(new FileReader(filename));
			String line;
			while (true)
			{
		        line = br.readLine();
		        if (line == null)
		        	break;
		        else
		        {
		        	N++;
		        	rows[i] = line.split(",");
		        	i++;
		        	if (i == rows.length)
		        		expandRows();
		        }
			}
		}
		finally {
			br.close();
		}

	}

	private void readFile(String filename, int maxRows) throws IOException {
		BufferedReader br = null;
		int i = 0;
		try
		{
			br = new BufferedReader(new FileReader(filename));
			String line;
			while (true)
			{
		        line = br.readLine();
		        if (line == null || i == maxRows)
		        	break;
		        else
		        {
		        	N++;
		        	rows[i] = line.split(",");
		        	i++;
		        	if (i == rows.length)
		        		expandRows();
		        }
			}
		}
		finally {
			br.close();
		}

	}

	private void expandRows()
	{
		String[][] aux = new String[rows.length][];
		for (int i = 0; i < rows.length; i++)
			aux[i] = rows[i];
		rows = new String[rows.length * 2][];
		for (int i = 0; i < aux.length; i++)
			rows[i] = aux[i];		
	}
	
	public boolean next(){
		C++;
		if (C < N)
			return true;
		return false;
	}
	
	public String[] getCurrentRow() {
		String[] row = new String[1];
		row = rows[C].clone();
		rows[C] = null;
		return row;
	}
	
	public int numOfRows(){
		return N;
	}
	
	public static DataLoader getInstance(String filename) throws IOException, ClassNotFoundException {
		System.out.println("Loading data ...");
		DataLoader dl = null;
		dl = new DataLoader(filename);
		return dl;
	}
	
	public static DataLoader getInstance(String filename, int rows) throws IOException, ClassNotFoundException {
		System.out.println("Loading data ...");
		DataLoader dl = null;
		dl = new DataLoader(filename, rows);
		return dl;
	}
	
	public static DataLoader getInstance(String query, Connection connection) throws SQLException {
		System.out.println("Loading data ...");
		DataLoader dl = null;
		dl = new DataLoader(query, connection);
		return dl;
	}	

}
