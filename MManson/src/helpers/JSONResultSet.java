package helpers;


public class JSONResultSet {
	private final String START = "{\"resultSet\":[\n";
	private final String END = "\n]}";
	
	private String line;
	
	public JSONResultSet(String[] keys){
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append( '{' );
		stringBuilder.append( quotedStr(keys[0]) );
		stringBuilder.append( ':' );
		stringBuilder.append( "%s" );
		for(int i = 1; i < keys.length; i++) {
			stringBuilder.append( "," );
			stringBuilder.append( quotedStr(keys[i]) );
			stringBuilder.append( ':' );
			stringBuilder.append( "%s" );
		}
		stringBuilder.append( '}' );
		line = stringBuilder.toString();		
	}
	
	private String quotedStr(String input) {
		if (input.toLowerCase().equals("null")) return input;
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append('"');
		stringBuilder.append(input);
		stringBuilder.append('"');
		return stringBuilder.toString();
	}
	
	private String[] quotedStr(String[] input) {
		for (int i = 0; i < input.length; i++)
			input[i] = quotedStr(input[i]);
		return input;
	}
	
	public String generate(String[][] resultSet) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(START);
						
		stringBuilder.append( String.format(line, (Object[])quotedStr(resultSet[0])) );	
		for (int i = 1; i < resultSet.length; i++){
			stringBuilder.append(",\n");			
			stringBuilder.append( String.format(line, (Object[])quotedStr(resultSet[i])) );
		}

		stringBuilder.append(END);
		return stringBuilder.toString();
	}
	
	public static void main(String[] args) {
		JSONResultSet j = new JSONResultSet(new String[]{"aa","bb"});
		String s = j.generate(new String[][]{{"1","2"},{"3","4"}});
		System.out.println(s);
	}
}
