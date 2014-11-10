import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Scanner;

//TODO unique columns (no need for rows array!!!)
//TODO first row occurrence (to improve performance by first index(rows array) lookup)
//TODO So many ST-related clashes!!! consider moving symbols to the trie maybe!!! like a field holding the symbol in the node and rows pointing to the node 
public class IdeenTrieC implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private final int NO_OF_DISTINCT_KEYS = 100;
	private final int NO_OF_ROWS = 1000000;	
	
	private class Node implements Serializable{

		private static final long serialVersionUID = 1L;

		private char[] key; //string memory : 40+2N bytes ==> should use char array (lose string convenience tho)
		
		private Node left;
		private Node right;
		private Node middle;

		// TODO row indexes
		//private String value;
		
		private int stIdx;
	}
	
	private Node root;
	private int letterCase = 0;
	private int N = 0; //num of distinct values
	private int M = 0; //num of distinct values
	
	private char[][] ST;
	
	private int[] ROWS;
	
	public IdeenTrieC(int letterCase, int numOfRows) {
		this.letterCase = letterCase; 
		
		if (numOfRows > 0) {
			ROWS = new int[numOfRows + 1];
			ST = new char[NO_OF_DISTINCT_KEYS][];
		}
		else {
			ROWS = new int[NO_OF_ROWS];
			ST = new char[NO_OF_DISTINCT_KEYS][];
		}
	}
	
	/**
	 * longest common prefix
	 * linear(worst case) / usually sub-linear (because we don't always have to iterate all chars)
	 * @param str1 String input string 1 
	 * @param str2 String input string 2 
	 * @return the longest common prefix of the two input strings
	 */
	private int lcp(char[] str1, char[] str2) 
	{
		int N = Math.min(str1.length, str2.length);
		for (int i = 0; i < N; i++)
			if (str1[i] != str2[i])
				return i;
		return N;
	}  
	
	private boolean bigger(char[] str1, char[] str2) {
		int i = 0;
		int N = Math.max(str1.length, str2.length);
		while(str1[i] == str2[i] && i < N) 
			i++;
		
		if (i == N)
			return str1.length > str2.length;  

		return str1[i] > str2[i];
	}

	private boolean smaller(char[] str1, char[] str2) {
		int i = 0;
		int N = Math.max(str1.length, str2.length);
		while(str1[i] == str2[i] && i < N) 
			i++;
		
		if (i == N)
			return str1.length < str2.length;  
		
		return str1[i] < str2[i];
	}

	private Node insertR(Node node, String key, int value) {
		if (node == null) {
			node = new Node();
			node.key = key.toCharArray();
			//node.value = String.valueOf(value);
			node.stIdx = ++N;
			ROWS[value] = node.stIdx; 
			return node;
		}
		
		int longestCommonPrefix = lcp(node.key, key.toCharArray());
		
		if (longestCommonPrefix > 0 && longestCommonPrefix == node.key.length && longestCommonPrefix == key.length()){ //exact match
			//node.value += ", " + String.valueOf(value);
			if (node.stIdx == 0)
				node.stIdx = ++N;
			ROWS[value] = node.stIdx;
		}
		/*else if (longestCommonPrefix > 0 && longestCommonPrefix == key.length())
			node.value = value;*/
		else if (longestCommonPrefix > 0 && longestCommonPrefix == node.key.length)
			node.middle = insertR(node.middle, key.substring(longestCommonPrefix), value);
		else if (longestCommonPrefix > 0)
			node = split(node, longestCommonPrefix, key, value);
		else if (smaller(key.toCharArray(), node.key))
			node.left = insertR(node.left, key, value);
		else if (bigger(key.toCharArray(), node.key))
			node.right = insertR(node.right, key, value);
		return node;
	}
	
	private Node split(Node node, int longestCommonPrefix, String key, int value) {
		char[] remainingNodeKey = CharArrayUtil.substring(node.key, longestCommonPrefix);
		String remainingKey = key.substring(longestCommonPrefix);
		char[] theLongestCommonPrefix = CharArrayUtil.substring(node.key, 0, longestCommonPrefix);
		
		node.key = theLongestCommonPrefix;
				
		Node nRNK = new Node();
		nRNK.key = remainingNodeKey;
		//nRNK.value = node.value;
		nRNK.middle = node.middle;
		nRNK.stIdx = node.stIdx;

		//node.value = "";
		node.stIdx = 0;
		node.middle = nRNK;
		
		Node nRK = null;
		if (remainingKey.length() > 0) {
			nRK = new Node();
			nRK.key = remainingKey.toCharArray();
			//nRK.value = String.valueOf(value);
			nRK.stIdx = ++N;

			if (smaller(nRK.key, nRNK.key)) 
				nRNK.left = nRK;
			else
				nRNK.right = nRK;
		}
		else {
			//node.value = String.valueOf(value);
			node.stIdx = ++N;
		}
		ROWS[value] = N;
		
		return node;
	}
	
	private Node find(Node node, String key) {
		if (node == null) return null;
		
		int longestCommonPrefix = lcp(node.key, key.toCharArray());
		
		if (longestCommonPrefix > 0 && longestCommonPrefix == node.key.length && longestCommonPrefix == key.length()) //exact match
			return node;

		else if (longestCommonPrefix > 0 && longestCommonPrefix == key.length())
			return null;

		else if (longestCommonPrefix > 0 && longestCommonPrefix == node.key.length)
			return find(node.middle, key.substring(longestCommonPrefix));

		else if (smaller(key.toCharArray(), node.key))
			return find(node.left, key);

		else if (bigger(key.toCharArray(), node.key))
			return find(node.right, key);
		
		return null;
	}
	
	private void collect(Node node, String prefix, ArrayList<String> keys) {
		if (node == null) return;
		
		collect(node.left, prefix, keys);
		
		if (node.stIdx > 0) //if (node.value.length() > 0)
			keys.add(prefix + new String(node.key));
		collect(node.middle, prefix + new String(node.key), keys);
		
		collect(node.right, prefix, keys);
	}
	
    private Node findFirstLevelNode(Node node, String key) {
        if (node == null) return null;

        int longestCommonPrefix = lcp(node.key, key.toCharArray());
       
        if (longestCommonPrefix > 0 && longestCommonPrefix == node.key.length && longestCommonPrefix == key.length()) //exact match
               return node;
       
        if (longestCommonPrefix > 0 && longestCommonPrefix == key.length() && key.length() < node.key.length) //key is part of this node
               return node;

        if (longestCommonPrefix > 0 && longestCommonPrefix == node.key.length && key.length() > node.key.length)
               return findFirstLevelNode(node.middle, key.substring(longestCommonPrefix)) != null ? node : null;
        		  //return findFirstLevelNode(node.middle, key.substring(longestCommonPrefix));	
        			
        if (smaller(key.toCharArray(), node.key))
               return findFirstLevelNode(node.left, key);
        else
               return findFirstLevelNode(node.right, key);
 }
 
	private void collectStartWith(Node node, String prefix, String masterPrefix, ArrayList<String> keys) {
		if (node == null) return;
		
		collectStartWith(node.left, prefix, masterPrefix, keys);
		String nodeRealValue = prefix + new String(node.key);
		if (node.stIdx > 0 && lcp(nodeRealValue.toCharArray(),masterPrefix.toCharArray()) == masterPrefix.length() )
			keys.add(nodeRealValue);
		collectStartWith(node.middle, nodeRealValue, masterPrefix, keys);
		
		collectStartWith(node.right, prefix, masterPrefix, keys);
	}
	
	private void collectContains(Node node, String prefix, String containsStr, ArrayList<String> keys) {
		if (node == null) return;
		
		collectContains(node.left, prefix, containsStr, keys);
		String nodeRealValue = prefix + new String(node.key);
		if (node.stIdx > 0 && nodeRealValue.contains(containsStr))
			keys.add(nodeRealValue);
		collectContains(node.middle, nodeRealValue, containsStr, keys);
		
		collectContains(node.right, prefix, containsStr, keys);
	}
	
	public void insert(String key, int value) {
		if (key == null || key.trim().length() == 0) key = "null";

		key = (letterCase == 1) ? key.toLowerCase() : key;
		
		root = insertR(root, key, value);
		
		if (N > M) {
			try{
				ST[++M] = key.toCharArray();
			}catch(ArrayIndexOutOfBoundsException e){
				//int E = 0;//(ROWS.length - value)/3;
				char[][] aux = new char[ST.length][];
				for(int i = 0; i < ST.length; i++)
					aux[i] = ST[i];
				ST = new char[ST.length + NO_OF_DISTINCT_KEYS][];
				for(int i = 0; i < aux.length; i++)
					ST[i] = aux[i];
			}
		}
	}
	
	public String getRowValue(int index) {
		return new String(ST[ROWS[index]]);
	}
	
	//TODO clash of function by ST!!!!! :)
	public String find(String key) {
		Node node = find(root, key);
		if (node != null && node.stIdx > 0)//if (node != null && node.value.length() != 0)
			return key + " : " + "found";//node.value;
		else
			return key + " - not found";
	}
	
	public int[] getRows(String key) {
		Node node = find(root, key);
		if (node != null && node.stIdx > 0) {
			StringBuilder val = new StringBuilder();
			int i = 1;
			while(ROWS[i] != node.stIdx)
				i++;
			val.append(i);
			for (int j = i+1; j < ROWS.length; j++)
				if (ROWS[j] == node.stIdx) {
					val.append(",");
					val.append(j);
				}
			String[] auxStrArray = val.toString().split(",");
			int[] rows = new int[auxStrArray.length];
			for (i = 0; i < auxStrArray.length; i++)
				rows[i] = Integer.parseInt(auxStrArray[i]);
			return rows;
		}
		else
			return new int[0];
	}
	
	public int[] getRows(String[] keys) {
		int[][] aux = new int[keys.length][];
		int i = 0;
		int N = 0;
		for (String key : keys) { 
			aux[i] = getRows(key);
			N += aux[i].length;
			i++;
		}
		
		int[] rows = new int[N];
		int rowsIndex = 0;
		
		for (int j = 0; j < i; j++)
			for (int k = 0; k < aux[j].length; k++)
				rows[rowsIndex++] = aux[j][k];
		return rows;
	}
	
	//TODO hey! why collect when ST is there!!!!
	public Iterable<String> keys()
	{ 
		ArrayList<String> keys = new ArrayList<String>();
		collect(root, "", keys);
		return keys;
	}
	
	public Iterable<String> keysStartWith(String prefix)
    {
           ArrayList<String> keys = new ArrayList<String>();
           Node node = findFirstLevelNode(root, prefix);
           if (node != null && lcp(node.key, prefix.toCharArray()) == prefix.length() && node.stIdx > 0)
        	   keys.add(new String(node.key));
           if (node != null)
        	   collectStartWith(node.middle, String.valueOf(node.key), prefix, keys);
           return keys;
    }
		
	public Iterable<String> keysContain(String containStr)
    {
           ArrayList<String> keys = new ArrayList<String>();
           collectContains(root, "", containStr, keys);
           return keys;
    }	
    
	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
		Stopwatch stopwatch = new Stopwatch();
		IdeenTrieC it = null;
				
		File f = new File("ideenTrie.dat");
		if (f.exists()) {
			FileInputStream file = new FileInputStream("ideenTrie.dat");
	        ObjectInputStream in = new ObjectInputStream(file);
	        it = (IdeenTrieC) in.readObject();
	        in.close();
	        file.close();
	        System.out.println("Deserialized...");
		}
		else {
			DataLoader dl = DataLoader.getInstance("attrs.csv");	
			it = new IdeenTrieC(1, dl.numOfRows());
			int rowCount = 0;
			try {
				while(dl.next()){ 		
					rowCount++;
					String[] row = dl.getCurrentRow();
					it.insert(row[2], rowCount);
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			
			System.gc();
			
			System.out.println(rowCount + " keys inserted");
			stopwatch.printElapsedtimeAndReset();
			FileOutputStream file = null;
			ObjectOutputStream out = null;
			try {
				file = new FileOutputStream("ideenTrie.dat");
				out = new ObjectOutputStream(file);
				out.writeObject(it);
				System.out.println("Serialized...");
				stopwatch.printElapsedtimeAndReset();
			}
			catch(OutOfMemoryError o) {
				System.out.println("Failed to serialize it data");
				o.printStackTrace();
				if (f.exists()) 
					f.delete();				
			}
			finally {
				out.close();
				file.close();				
			}
		}
		
		Scanner scanner = new Scanner(System.in);
		boolean b = true;
		while(b) {
			System.out.println("----------------------");
			System.out.println("0-Exit");
			System.out.println("1-List all keys");
			System.out.println("2-Look for a key");
			System.out.println("3-List all keys which begin with a specific prefix");
			System.out.println("4-List all keys which contain a specific substring");
			System.out.print("Choice:");
			try {
				int input = scanner.nextInt();
				switch (input) {
				case 0:
					b = false;
					break;
				case 1:{
					stopwatch.reset();
					Iterable<String> keys = it.keys();
					int i = 0;
					for (String key : keys) 
						System.out.println(++i + "- " + key);
					stopwatch.printElapsedtime();
					break;
				}
				case 2:{
					System.out.print("Key : ");
					String key = scanner.next();
					stopwatch.reset();
					System.out.println("***********************");
					System.out.println(it.find(key));
					System.out.println("***********************");
					stopwatch.printElapsedtimeInMillisAndReset();
					System.out.println("***********************");
					System.out.println(it.getRows(key));
					System.out.println("***********************");
					stopwatch.printElapsedtimeInMillisAndReset();
					break;
				}
				case 3:{
					int j = 0;
					System.out.print("Prefix : ");
					String prefix = scanner.next();
					stopwatch.reset();
					Iterable<String> prefixedKeys = it.keysStartWith(prefix);
					for (String prefixedKey : prefixedKeys) 
						System.out.println(++j + "- " + prefixedKey);
					stopwatch.printElapsedtime();					
					break;
				}
				case 4:{
					int k = 0;
					System.out.print("Contain Str : ");
					String containStr = scanner.next();
					stopwatch.reset();
					Iterable<String> containedKeys = it.keysContain(containStr);
					for (String containedKey : containedKeys) 
						System.out.println(++k + "- " + containedKey);
					stopwatch.printElapsedtime();					
					break;
				}
				default:
					System.out.println("Invalid Choice");
					break;
				} 
			}
			catch(InputMismatchException e) {
				System.out.println("Invalid Choice");
				scanner.next();
			}
			System.out.println(" ");
			System.out.println(" ");
		}
		scanner.close();
	}
}
