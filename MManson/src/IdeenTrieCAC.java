import java.io.Serializable;
import java.util.ArrayList;
import java.util.Scanner;

public class IdeenTrieCAC implements Serializable {

	private static final long serialVersionUID = 1L;

	private class Node implements Serializable {
		private static final long serialVersionUID = 1L;
		private char key; // string memory : 40+2N bytes ==> should use char
							// array (lose string convenience tho)
		private Node left;
		private Node right;
		private Node middle;
		private int stIdx;
	}

	private Node root;

	private int letterCase = 0;
	private int N = 0; // num of distinct values

	//private ByteBufferInterface[] ST;
	// private byte[][] ST;

	public IdeenTrieCAC() {
		this.letterCase = 1;

	}

	private Node insertR(Node node, String key, int d) {
		char c = key.charAt(d);
		if (node == null) {
			node = new Node();
			node.key = c;
		}

		if (c < node.key)
			node.left = insertR(node.left, key, d);
		else if (c > node.key)
			node.right = insertR(node.right, key, d);
		else if (d < key.length() - 1)
			node.middle = insertR(node.middle, key, d+1);
		else {
			node.stIdx = ++N;
			//TODO ST
		}			
		return node;
	}

	private Node find(Node node, String key, int d) {
		if (node == null) return null;
		char c = key.charAt(d);
		
		if (c < node.key) return find(node.left, key, d);
		else if (c > node.key) return find(node.right, key, d);
		else if (d < key.length() - 1) return find(node.middle, key, d+1);
		else return node;
	}

	private void collect(Node node, String prefix, ArrayList<String> keys) {
		if (node == null) return;

		collect(node.left, prefix, keys);

		if (node.stIdx > 0)
			keys.add(prefix + node.key);
		collect(node.middle, prefix + node.key, keys);

		collect(node.right, prefix, keys);
	}
	
	public void insert(String key) {
		if (key == null || key.trim().length() == 0)
			key = "null";

		key = (letterCase == 1) ? key.toLowerCase() : key;

		key = key.trim();

		root = insertR(root, key, 0);
	}

	public String find(String key) {
		Node node = find(root, key, 0);
		if (node != null && node.stIdx > 0)
			return key + " : " + "found";
		else
			return key + " - not found";
	}
	
	public Iterable<String> keys() {
		ArrayList<String> keys = new ArrayList<String>();
		collect(root, "", keys);
		return keys;
	}

	public static void main(String[] args) {
		
		/*Scanner scanner = new Scanner(System.in);
		boolean b = true;
		while (b) {
			char key = scanner.next		
		}
		scanner.close();*/
		IdeenTrieCAC it = new IdeenTrieCAC();
		it.insert("adidas");
		it.insert("Innovare Made in Ita");
		it.insert("Julius Marlow");
		it.insert("Tony Bianco");
		it.insert("Sandler");
		it.insert("Easy Steps");
		it.insert("Kenji");
		it.insert("Vince Camuto");
		it.insert("Studio Pollini");
		it.insert("Tommy Tickle");
		it.insert("Robert Clergerie");
		it.insert("Earth");
		it.insert("Converse");
		it.insert("Converse");
		it.insert("Osh Kosh");
		it.insert("Urban Soul");
		it.insert("Easy Steps");
		it.insert("Trent Nathan");
		it.insert("Windsor Smith");
		it.insert("Timberland");
		it.insert("Clarks");
		it.insert("Barbara Bui");
		it.insert("Wide Steps");
		it.insert("The Flexx");
		it.insert("Lacoste");
		it.insert("Rodd & Gunn");
		it.insert("Angel Azorin");
		it.insert("Zensu");
		it.insert("Mossimo");
		it.insert("Slazenger");
		it.insert("Easy Steps");
		it.insert("Moschino Cheap and C");

		System.out.println(it.find("windsor smith"));
		System.out.println(it.find("windsor smiths"));
		System.out.println(it.find("windsor smi"));
	}
}
