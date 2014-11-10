import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
	
	/*public String[] getRow(int i) {
		String[] row = new String[1];
		row = rows[i].clone();
		rows[i] = null;
		return row;
	}*/
	
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
		/*File f = new File(filename+".dat");
		if (f.exists()) {
			FileInputStream file = new FileInputStream(filename+".dat");
	        ObjectInputStream in = new ObjectInputStream(file);
	        dl = (DataLoader)in.readObject();
	        in.close();
	        file.close();
	        System.out.println("DL Deserialized...");				
		}
		else {
			FileOutputStream file = null;
			ObjectOutputStream out = null;
			try {
				dl = new DataLoader(filename);
				file = new FileOutputStream(filename+".dat");
				out = new ObjectOutputStream(file);
				out.writeObject(dl);
				System.out.println("Loaded");
			}
			catch(OutOfMemoryError o){
				System.out.println("Failed to serialize raw data");
				o.printStackTrace();
				f = new File(filename+".dat");
				if (f.exists()) 
					f.delete();
			}
			finally{
				out.close();
				file.close();
			}
		}*/

		return dl;
	}
}
