package controlF;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.sql.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


/**
 * Universal database methods.
 * 1) Creates JDBC connection
 * 2) Return JSON from a select statement
 * 3) Insert/update/delete statements
 * @author Jessica
 *
 */
public class Database {
	
	/**
	 * Create MySQL JDBC connection
	 * @return Connection
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	private static Connection connect_database() throws IOException, ClassNotFoundException, SQLException {
		/* Read config file for database credentials */
		Properties prop = new Properties();
		FileInputStream in = new FileInputStream("/Users/macintosh/Documents/Eclipse/ControlFJava/resources/database.properties");
		prop.load(in);
		final String HOST = prop.getProperty("dbHostName");
		final String USERNAME = prop.getProperty("dbUsername");
		final String PASSWORD = prop.getProperty("dbPassword");
		final String PORT_NUMBER = prop.getProperty("dbPortNumber");
		final String DB_NAME = prop.getProperty("dbName");
		
		/* Create JDBC connection to MySQL database */
		Class.forName("com.mysql.jdbc.Driver");
		String dbUrl = "jdbc:mysql://"+HOST+":/"+PORT_NUMBER+"/"+DB_NAME; 
		Connection conn = DriverManager.getConnection(dbUrl, USERNAME, PASSWORD);
		return conn;
	}
	
	/**
	 * Run SQL select statement that returns data in JSON form
	 * @param String query
	 * @return JSONObject
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static JSONObject return_data(String query) throws ClassNotFoundException, IOException, SQLException {
		Connection conn = connect_database();
		Statement s = conn.createStatement();
		ResultSet r = s.executeQuery(query);
		ResultSetMetaData m = r.getMetaData();
		JSONObject data = null;
		JSONArray arr = null;
		
		while (r.next()) {
			JSONObject obj = null;
			for (int i = 0; i < m.getColumnCount(); i++) {
				obj.put(m.getColumnLabel(i), r.getString(i));
			}
			arr.add(obj);
		}
		data.put("DATA", arr);
		
		return data;
	}
	
	/**
	 * Run SQL insert / delete / update statements
	 * No return data
	 * @param String query
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static void insert_delete_update_records(String query) throws SQLException, ClassNotFoundException, IOException {
		Connection conn = connect_database();
		Statement s = conn.createStatement();
		s.executeQuery(query);
		System.out.println("Records updated/inserted/deleted.");
	}
}
