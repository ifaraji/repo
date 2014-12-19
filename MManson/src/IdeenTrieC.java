import helpers.ByteUtils;
import helpers.CharArrayUtils;
import helpers.DataLoader;
import helpers.IntArrayUtils;
import helpers.Stopwatch;

import java.io.File;
import java.io.FileInputStream;
//import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Scanner;

import bytebuffers.AbstractByteBuffer;
import bytebuffers.ByteBufferI;
import bytebuffers.ByteBufferInterface;

public class IdeenTrieC implements Serializable {

	private static final long serialVersionUID = 1L;
	// private final int NO_OF_DISTINCT_KEYS = 100;
	private final int NO_OF_ROWS = 1000000;

	private class Node implements Serializable {
		private static final long serialVersionUID = 1L;
		private char[] key; // string memory : 40+2N bytes ==> should use char
							// array (lose string convenience tho)
		private Node left;
		private Node right;
		private Node middle;
		private int stIdx;
		private int O;
	}

	private Node root;
	private boolean UK;

	private int letterCase = 0;
	private int N = 0; // num of distinct values

	private ByteBufferInterface[] ST;
	// private byte[][] ST;
	private int[] ROWS;

	public IdeenTrieC(int numOfRows, boolean unique) {
		this.UK = unique;
		this.letterCase = 1;

		if (!UK) {
			if (numOfRows > 0)
				ROWS = new int[numOfRows + 1];
			else
				ROWS = new int[NO_OF_ROWS];
		}
	}

	private Node insertR(Node node, String key, int value) {
		if (node == null) {
			node = new Node();
			node.key = key.toCharArray();
			node.stIdx = ++N;
			node.O++;
			if (!UK)
				ROWS[value] = node.stIdx;
			return node;
		}

		int longestCommonPrefix = CharArrayUtils.lcp(node.key,
				key.toCharArray());

		if (longestCommonPrefix > 0 && longestCommonPrefix == node.key.length
				&& longestCommonPrefix == key.length()) { // exact match
			if (node.stIdx == 0)
				node.stIdx = ++N;
			node.O++;
			if (!UK)
				ROWS[value] = node.stIdx;
		} else if (longestCommonPrefix > 0
				&& longestCommonPrefix == node.key.length)
			node.middle = insertR(node.middle,
					key.substring(longestCommonPrefix), value);
		else if (longestCommonPrefix > 0)
			node = split(node, longestCommonPrefix, key, value);
		else if (CharArrayUtils.smaller(key.toCharArray(), node.key))
			node.left = insertR(node.left, key, value);
		else if (CharArrayUtils.bigger(key.toCharArray(), node.key))
			node.right = insertR(node.right, key, value);
		return node;
	}

	private Node split(Node node, int longestCommonPrefix, String key, int value) {
		char[] remainingNodeKey = CharArrayUtils.substring(node.key,
				longestCommonPrefix);
		String remainingKey = key.substring(longestCommonPrefix);
		char[] theLongestCommonPrefix = CharArrayUtils.substring(node.key, 0,
				longestCommonPrefix);

		node.key = theLongestCommonPrefix;

		Node nRNK = new Node();
		nRNK.key = remainingNodeKey;
		nRNK.middle = node.middle;
		nRNK.stIdx = node.stIdx;
		nRNK.O = node.O;

		node.stIdx = 0;
		node.O = 0;
		node.middle = nRNK;

		Node nRK = null;
		if (remainingKey.length() > 0) {
			nRK = new Node();
			nRK.key = remainingKey.toCharArray();
			nRK.stIdx = ++N;
			nRK.O++;

			if (CharArrayUtils.smaller(nRK.key, nRNK.key))
				nRNK.left = nRK;
			else
				nRNK.right = nRK;
		} else {
			node.stIdx = ++N;
			node.O++;
		}
		if (!UK)
			ROWS[value] = N;

		return node;
	}

	private void buildST(Node node, String path) {
		if (node == null)
			return;

		buildST(node.left, path + "L");

		if (node.stIdx > 0) {
			ST[node.stIdx] = ByteBufferI.getByteBuffer(ByteUtils
					.pathToByteArray(path));
			((AbstractByteBuffer) ST[node.stIdx]).createRows(node.O);
		}

		buildST(node.middle, path + "M");

		buildST(node.right, path + "R");
	}

	private void buildST2() {
		if (!UK)
			for (int i = 1; i < ROWS.length; i++)
				((AbstractByteBuffer) ST[ROWS[i]]).addRow(i);
		else
			for (int i = 1; i < ST.length; i++)
				((AbstractByteBuffer) ST[i]).addRow(i);
	}

	private String resolveSTValue(byte[] input) {
		String path = ByteUtils.byteArrayToPath(input);
		return resolveSTValue(root, path, 0);
	}

	private String resolveSTValue(Node node, String path, int step) {
		if (node == null)
			return "";

		StringBuilder sb = new StringBuilder();
		if (step == path.length()) {
			sb.append(node.key);
		} else if (path.charAt(step) == 'L') {
			String s = resolveSTValue(node.left, path, ++step);
			sb.append(s);
		} else if (path.charAt(step) == 'M') {
			sb.append(node.key);
			String s = resolveSTValue(node.middle, path, ++step);
			sb.append(s);
		} else if (path.charAt(step) == 'R') {
			String s = resolveSTValue(node.right, path, ++step);
			sb.append(s);
		}

		return sb.toString();
	}

	private Node find(Node node, String key) {
		if (node == null) return null;

		int lcp = CharArrayUtils.lcp(node.key, key.toCharArray());

		if (lcp > 0 && lcp < node.key.length && lcp < key.length()) // key does not exist. if it did, this node would be splitted
			return null;

		if (lcp > 0 && lcp == node.key.length && lcp == key.length()) // exact match
			return node;

		else if (lcp > 0 && lcp == key.length()) // key is part of the current node
			return null;

		else if (lcp > 0 && lcp == node.key.length) // if exists at all, the key will be somewhere down here			
			return find(node.middle, key.substring(lcp));

		else if (CharArrayUtils.smaller(key.toCharArray(), node.key)) //lcp = 0
			return find(node.left, key);

		else if (CharArrayUtils.bigger(key.toCharArray(), node.key)) //lcp = 0
			return find(node.right, key);

		return null;
	}

	private void collect(Node node, String prefix, ArrayList<String> keys) {
		if (node == null) return;

		collect(node.left, prefix, keys);

		if (node.stIdx > 0)
			keys.add(prefix + new String(node.key));
		collect(node.middle, prefix + new String(node.key), keys);

		collect(node.right, prefix, keys);
	}

	private Node findStartWith(Node node, String key, ArrayList<String> nodeValue) {
		if (node == null) return null;

		int lcp = CharArrayUtils.lcp(node.key, key.toCharArray());

		if (lcp > 0 && lcp < node.key.length && lcp < key.length()) // key does not exist. if it did, this node would be splitted
			return null;

		if (lcp > 0 && lcp == node.key.length && lcp == key.length()) { // exact match
			nodeValue.add(new String(node.key));
			return node;
		}

		if (lcp > 0 && lcp == key.length() && key.length() < node.key.length) { // key is part of this node
			nodeValue.add(new String(node.key));
			return node;
		}

		if (lcp > 0 && lcp == node.key.length && key.length() > node.key.length) { // if at all, key is somewhere down here
			nodeValue.add(new String(node.key));
			return findStartWith(node.middle, key.substring(lcp), nodeValue);
		}

		if (CharArrayUtils.smaller(key.toCharArray(), node.key))
			return findStartWith(node.left, key, nodeValue);
		else
			return findStartWith(node.right, key, nodeValue);
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
	
	//TODO void collectContains(Node node, String prefix, String key, int check, ArrayList<String> keys) //just to reduce the 3 of contains. thats all
	private void collectContains(Node node, String prefix, String key, boolean check, ArrayList<String> keys) {
		/*if (node == null) return;

		String nodeRealValue = prefix + new String(node.key);
		
		if(check) 
			if (node.stIdx > 0)
				keys.add(nodeRealValue);
		
		if (nodeRealValue.contains(key)) check = true;
		
		if (node.stIdx > 0 && keyCharAt == key.length())
			keys.add(nodeRealValue);

		collectContains(node.left, prefix, key, keyCharAt, keys);
		collectContains(node.middle, nodeRealValue, key, keyCharAt, keys);
		collectContains(node.right, prefix, key, keyCharAt, keys);*/
	}

	private int[] collectContains(Node node, String prefix, String key) {
		if (node == null) return null;

		int[] left = collectContains(node.left, prefix, key);
		
		String nodeRealValue = prefix + new String(node.key);
		int[] self = null;
		if (node.stIdx > 0 && nodeRealValue.contains(key))
			self = ((AbstractByteBuffer) ST[node.stIdx]).getRows();
		
		int[] middle = collectContains(node.middle, nodeRealValue, key);

		int[] right = collectContains(node.right, prefix, key);

		self = IntArrayUtils.merge(self, left);
		self = IntArrayUtils.merge(self, middle);
		self = IntArrayUtils.merge(self, right);

		return self;
	}
	
	//TODO int[] collectContains(Node node, String prefix, String key, int keyCharAt)
	private int[] collectContains(Node node, String prefix, String key, int keyCharAt) {
		if (node == null) return null;

		int lcp = CharArrayUtils.lcp(node.key, key.substring(keyCharAt).toCharArray());
		if (lcp > 0 && lcp == node.key.length)
			keyCharAt = lcp;
		if (lcp > 0 && lcp == key.length())
			keyCharAt = lcp;
		
		int[] self = null;
		if (node.stIdx > 0 && keyCharAt == key.length())
			self = ((AbstractByteBuffer) ST[node.stIdx]).getRows();

		int[] left = collectContains(node.left, prefix, key, keyCharAt);
		String nodeRealValue = prefix + new String(node.key);
		int[] middle = collectContains(node.middle, nodeRealValue, key, keyCharAt);
		int[] right = collectContains(node.right, prefix, key, keyCharAt);

		self = IntArrayUtils.merge(self, left);
		self = IntArrayUtils.merge(self, middle);
		self = IntArrayUtils.merge(self, right);

		return self;
	}

	private int[] collectContains(Node node, String prefix, String[] keys) {
		if (node == null) return null;

		int[] left = collectContains(node.left, prefix, keys);
		
		String nodeRealValue = prefix + new String(node.key);
		int[] self = null;
		if (node.stIdx > 0 ) { 
			boolean b = true;
			for (String key : keys)//TODO Seriously? Regex at least!!!
				b = b && nodeRealValue.contains(key);
			if (b)
				self = ((AbstractByteBuffer) ST[node.stIdx]).getRows();
		}
		
		int[] middle = collectContains(node.middle, nodeRealValue, keys);

		int[] right = collectContains(node.right, prefix, keys);

		self = IntArrayUtils.merge(self, left);
		self = IntArrayUtils.merge(self, middle);
		self = IntArrayUtils.merge(self, right);

		return self;
	}

	public void insert(String key, int value) {
		if (key == null || key.trim().length() == 0)
			key = "null";

		key = (letterCase == 1) ? key.toLowerCase() : key;

		key = key.trim();

		root = insertR(root, key, value);
	}

	public void finalize() {
		ST = new ByteBufferInterface[N + 1];
		buildST(root, "");
		buildST2();
	}

	public String getRowValue(int index) {
		if (UK)
			return resolveSTValue(ST[index].getData());
		else
			return resolveSTValue(ST[ROWS[index]].getData());
	}

	public String find(String key) {
		Node node = find(root, key);
		if (node != null && node.stIdx > 0)
			return key + " : " + "found";
		else
			return key + " - not found";
	}
	
	//TODO longestPrefixOf
	public String longestPrefixOf(String key) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	public int[] getRows(String key) {
		Node node = find(root, key);
		if (node != null && node.stIdx > 0) {
			return ((AbstractByteBuffer) ST[node.stIdx]).getRows();
		} else
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

	public int[] getRowsContaining(String key) {
		return collectContains(root, "", key);
	}

	public int[] getRowsContaining(String keys, String delimiter) {
		return collectContains(root, "", keys.split(delimiter));
	}

	// TODO getRowsContainingPattern
	public int[] getRowsContainingPattern(String regex) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	public Iterable<String> keys() {
		ArrayList<String> keys = new ArrayList<String>();
		collect(root, "", keys);
		return keys;
	}

	public Iterable<String> keysStartWith(String prefix) {
		ArrayList<String> keys = new ArrayList<String>(); 
		ArrayList<String> nodeValue = new ArrayList<String>();
		Node node = findStartWith(root, prefix, nodeValue);
		if (node != null) {
			StringBuilder stringBuilder = new StringBuilder();
			for (int i = 0; i < nodeValue.size(); i++)
				stringBuilder.append(nodeValue.get(i));			
			if (node.stIdx > 0)
				keys.add(stringBuilder.toString());
			collect(node.middle, stringBuilder.toString(), keys);
		}
		return keys;
	}

	public Iterable<String> keysContain(String containStr) {
		ArrayList<String> keys = new ArrayList<String>();
		collectContains(root, "", containStr, keys);
		return keys;
	}

	public static void main(String[] args) throws SQLException, IOException,
			ClassNotFoundException {
		Stopwatch stopwatch = new Stopwatch();
		IdeenTrieC it = null;

		File f = new File("..\\..\\ideenTrie.dat");
		if (f.exists()) {
			FileInputStream file = new FileInputStream("..\\..\\ideenTrie.dat");
			ObjectInputStream in = new ObjectInputStream(file);
			it = (IdeenTrieC) in.readObject();
			in.close();
			file.close();
			System.out.println("Deserialized...");
		} else {
			DataLoader dl = DataLoader.getInstance("..\\..\\attrs.csv");
			it = new IdeenTrieC(/*dl.numOfRows()*/ 20, false);
			int rowCount = 0;
			String[] row = null;
			try {
				while (dl.next() && rowCount < 20 ) {
					rowCount++;
					row = dl.getCurrentRow();
					it.insert(row[2]/*.substring(1, row[2].length() - 1)*/, rowCount);
				}
				it.finalize();
			} catch (Exception e) {
				System.out.println(rowCount + " " + row[2]);
				e.printStackTrace();
				throw e;
			}

			System.out.println(rowCount + " keys inserted");
			stopwatch.printElapsedtimeAndReset();
			/*FileOutputStream file = null;
			ObjectOutputStream out = null;
			try {
				file = new FileOutputStream("..\\..\\ideenTrie.dat");
				out = new ObjectOutputStream(file);
				out.writeObject(it);
				System.out.println("Serialized...");
				stopwatch.printElapsedtimeAndReset();
			} catch (OutOfMemoryError o) {
				System.out.println("Failed to serialize it data");
				o.printStackTrace();
				if (f.exists())
					f.delete();
			} finally {
				out.close();
				file.close();
			}*/
		}

		/*
		 * FileOutputStream file = null; ObjectOutputStream out = null; file =
		 * new FileOutputStream("..\\..\\ideenTrie.rows.dat"); out = new
		 * ObjectOutputStream(file); out.writeObject(it.ROWS); out.close();
		 * file.close(); file = new
		 * FileOutputStream("..\\..\\ideenTrie.st.dat"); out = new
		 * ObjectOutputStream(file); out.writeObject(it.ST); out.close();
		 * file.close();
		 */

		/*
		 * for (int h = 1; h <= 180; h++) System.out.println(h + ": "+
		 * it.getRowValue(h)); stopwatch.printElapsedtimeInMillisAndReset();
		 */

		Scanner scanner = new Scanner(System.in);
		boolean b = true;
		while (b) {
			System.out.println("----------------------");
			System.out.println("0-Exit");
			System.out.println("1-List all keys");
			System.out.println("2-Look for a key");
			System.out.println("3-List all keys which begin with a specific prefix");
			System.out.println("4-List all keys which contain a specific substring");
			System.out.println("5-Get row value");
			System.out.print("Choice:");
			try {
				int input = scanner.nextInt();
				switch (input) {
				case 0:
					b = false;
					break;
				case 1: {
					stopwatch.reset();
					Iterable<String> keys = it.keys();
					int i = 0;
					for (String key : keys)
						System.out.println(++i + "- " + key);
					stopwatch.printElapsedtimeInMillisAndReset();
					break;
				}
				case 2: {
					System.out.print("Key : ");
					String key = scanner.next();
					stopwatch.reset();
					System.out.println("***********************");
					System.out.println(it.find(key));
					System.out.println("***********************");
					stopwatch.printElapsedtimeInMillisAndReset();
					System.out.println("***********************");
					System.out.println(IntArrayUtils.intArrayToString(it.getRows(key)));
					System.out.println("***********************");
					stopwatch.printElapsedtimeInMillisAndReset();
					break;
				}
				case 3: {
					int j = 0;
					System.out.print("Prefix : ");
					String prefix = scanner.next();
					stopwatch.reset();
					Iterable<String> prefixedKeys = it.keysStartWith(prefix);
					for (String prefixedKey : prefixedKeys)
						System.out.println(++j + "- " + prefixedKey);
					stopwatch.printElapsedtimeInMillisAndReset();
					break;
				}
				case 4: {
					int k = 0;
					System.out.print("Contain Str : ");
					String containStr = scanner.next();
					stopwatch.reset();
					Iterable<String> containedKeys = it.keysContain(containStr);
					for (String containedKey : containedKeys)
						System.out.println(++k + "- " + containedKey);
					stopwatch.printElapsedtimeInMillisAndReset();
					break;
				}
				case 5: {
					System.out.print("Row : ");
					String key = scanner.next();
					stopwatch.reset();
					System.out.println("***********************");
					System.out.println(it.getRowValue(Integer.parseInt(key)));
					System.out.println("***********************");
					stopwatch.printElapsedtimeInMillisAndReset();
					break;
				}
				default:
					System.out.println("Invalid Choice");
					break;
				}
			} catch (InputMismatchException e) {
				System.out.println("Invalid Choice");
				scanner.next();
			}
			System.out.println(" ");
			System.out.println(" ");
		}
		scanner.close();
	}
}
