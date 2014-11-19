package helpers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class DBConnection {
	public static Connection getMRPSConnection() throws SQLException {
		Connection connection = null;
		Properties connectionProps = new Properties();
	    connectionProps.put("user", "mrps");
	    connectionProps.put("password", "mrps");
	    
	    DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
	    
	    connection = DriverManager.getConnection("jdbc:oracle:thin:@uqmmdb23:1521:ORRMYRP", connectionProps);
	    
	    if (connection == null)
	    	throw new RuntimeException("Unable to connect to the DB");
	    return connection;
	}
	
	public static Connection getMYRMSConnection() throws SQLException {
		Connection connection = null;
		Properties connectionProps = new Properties();
	    connectionProps.put("user", "myrms");
	    connectionProps.put("password", "k33pusers0ut");
	    
	    DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
	    
	    connection = DriverManager.getConnection("jdbc:oracle:thin:@uqmmdb10:1521:ORQMYMS", connectionProps);
	    
	    if (connection == null)
	    	throw new RuntimeException("Unable to connect to the DB");
	    return connection;
	}	
}
