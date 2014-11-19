import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;


public class DataLoader implements Serializable
{
	private static final long serialVersionUID = 1L;
	private String[][] rows;
	private int N;
	private int C;
	private DataLoader(String filename) throws IOException {
		rows = new String[10000][];
		N = 0;
		C = -1;
		readFile(filename);
		System.out.println(numOfRows() + " rows loaded");
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
}
