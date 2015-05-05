package helpers;

import java.util.Arrays;

public class TableObj {
	 public String name;
	 public String[] columns;
	 public String query;
	 
	 public String toString(){
		 return name + "\n" +
				 query + "\n" +
				 Arrays.deepToString(columns);
	 }
}
