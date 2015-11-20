package com.sb.db.test;

import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Row;
import com.sb.db.cassandra.CassandraHandlerDS;
import com.sb.db.cassandra.CassandraHandlerJDBC;
import com.sb.db.cassandra.DBProperties;
import com.sb.db.cassandra.DataValue;
import com.sb.utils.Utils;

/**
 * this is a test class to test the db operations using both the methods i.e. jdbc driver and datastax driver
 * 
 * @author bizz.sand
 */
public class TestCassandraDB {
	
	/**
	 * @param args String[]
	 */
	@SuppressWarnings("javadoc")
	public static void main(String[] args) {
		Utils.write2Console("___________________JDBC METHOD STARTS______________________");
		testCassandraDBJDBC("username", "password", "127.0.0.1", "9160", "mykeyspace");
		Utils.write2Console("___________________JDBC METHOD ENDS________________________");
		
		Utils.write2Console("___________________DS METHOD STARTS______________________");
		testCassandraDBDS("username", "password", "127.0.0.1", "9042", "mykeyspace");
		Utils.write2Console("___________________DS METHOD ENDS________________________");
	}
	
	/**
	 * this test method is used to test cassandra database operations using JDBC approach
	 * @param sDBUsername String
	 * @param sDBPassword String
	 * @param sNode String
	 * @param sPort String
	 * @param sKeyspaceName String
	 */
	@SuppressWarnings({ "unused", "rawtypes", "unchecked"})
	public static void testCassandraDBJDBC(String sDBUsername, String sDBPassword, String sNode, String sPort, String sKeyspaceName) {
		// TODO Auto-generated method stub
		String sTableName = "TEST_RUN_TABLE";
		
		// ----------------------------------------------------
		// set the cassandra database properties
		// ----------------------------------------------------
		Utils.write2Console("SETTING DATABASE PROPERTIES.........");
		DBProperties DBP = new DBProperties();
		DBP.setUsername(sDBUsername);
		DBP.setPassword(sDBPassword);
		DBP.setNode(sNode);
		DBP.setPort(sPort);
		DBP.setKeyspace(sKeyspaceName);
		
		CassandraHandlerJDBC ch = new CassandraHandlerJDBC(DBP);
		try {
			
			// ----------------------------------------------------
			// connect to the data base by providing the database properties
			// ----------------------------------------------------
			Utils.write2Console("CONNECTING TO THE DATABASE.........");
			ch.connect2DB();
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// DROP the table
			// ----------------------------------------------------
			Utils.write2Console("DROPPING TABLE >>> " + sTableName + " <<<");
			ch.processDDLData("DROP TABLE IF EXISTS " + sTableName);
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// CREATE the table
			// ----------------------------------------------------
			Utils.write2Console("CREATING TABLE >>> " + sTableName + " <<<");
			ch.processDMLData("CREATE TABLE " + sTableName + "(userid int, firstname text, lastname text, age int, primary key (userid))");
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// delete a row
			// ----------------------------------------------------
			Utils.write2Console("DELETING THE ROWS FROM >>> " + sTableName + " <<<");
			ch.processDMLData("DELETE FROM " + sTableName + " where userid in (0, 5000)");
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// create the rows
			// ----------------------------------------------------
			Utils.write2Console("CREATING THE ROWS (65535) IN >>> " + sTableName + " <<<");
			ArrayList alInsert = new ArrayList();
			for (int k = 0; k < 65534; k++) {
				alInsert.add("INSERT INTO " + sTableName + " (userid, firstname, lastname, age) VALUES (" + k + ", 'abc', 'def'," + k + ")");
			}
			ch.processDMLBatchData(alInsert);
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			alInsert = null;
			
			// ----------------------------------------------------
			// update the rows
			// ----------------------------------------------------
			Utils.write2Console("UPDATING THE ROWS IN >>> " + sTableName + " <<<");
			ch.processDMLData("UPDATE " + sTableName + " SET firstname = 'test123' WHERE userid in (0, 5000)");
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// [METHOD 1] read the rows
			// ----------------------------------------------------
			int iRowCount = 0;
			Utils.write2Console("(METHOD 1) READING THE comma separated ROWS (65535) FROM >>> " + sTableName + " <<<");
			ResultSet rs = ch.exQuery("select * from " + sTableName);
			while (rs.next()) {
				iRowCount += 1;
			}
			Utils.write2Console("(METHOD 1) READ " + iRowCount + " rows");
			
			// ----------------------------------------------------
			// [METHOD 2] read the rows
			// ----------------------------------------------------
			iRowCount = 0;
			Utils.write2Console("(METHOD 2) READING THE DataValue structure ROWS (65535) FROM >>> " + sTableName + " <<<");
			ArrayList alTable = ch.getData("select * from " + sTableName);
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// if the query was successful then CassandraHandler's alData should
			// have the data populated
			Utils.write2Console("(METHOD 2) ITERATING THE DataValue structure<<<");
			if (!alTable.isEmpty()) {
				int alSize = alTable.size();
				for (int i = 0; i < alSize; i++) {
					ArrayList alData = (ArrayList) ch.getAlData().get(i);
					int adSize = alData.size();
					for (int j = 0; j < adSize; j++) {
						DataValue dv = (DataValue) alData.get(j);
						// System.out.print(dv.getColumnFieldName() + "|" + dv.getColumnFieldType()
						// + "|" + dv.getColumnFieldValue());
						// System.out.print(" ; ");
					}
					// System.out.println(" ");
					alData = null;
					iRowCount += 1;
				}
			}
			Utils.write2Console("(METHOD 2) READ " + iRowCount + " rows");
			
			// ----------------------------------------------------
			// close the connection
			// ----------------------------------------------------
			Utils.write2Console("CLOSING THE DATABASE CONNECTION........");
			ch.closeConnection();
			ch = null;
		} catch (Exception e) {
			e.printStackTrace();
			try {
				ch.closeConnection();
			} catch (Exception x) {
				
			}
		}
	}
	
	/**
	 * this test method is used to test cassandra database operations using datastax approach
	 * @param sDBUsername String
	 * @param sDBPassword String
	 * @param sNode String
	 * @param sPort String
	 * @param sKeyspaceName String
	 */
	@SuppressWarnings({ "unused", "rawtypes", "unchecked" })
	public static void testCassandraDBDS(String sDBUsername, String sDBPassword, String sNode, String sPort, String sKeyspaceName) {
		// TODO Auto-generated method stub
		String sTableName = "TEST_RUN_TABLE";
		
		// ----------------------------------------------------
		// set the cassandra database properties
		// ----------------------------------------------------
		Utils.write2Console("SETTING DATABASE PROPERTIES.........");
		DBProperties DBP = new DBProperties();
		DBP.setUsername(sDBUsername);
		DBP.setPassword(sDBPassword);
		
		Collection<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
		
		addresses.add(new InetSocketAddress(sNode, Integer.parseInt(sPort)));
		DBP.setAddresses(addresses);		
		DBP.setKeyspace(sKeyspaceName);
		
		CassandraHandlerDS ch = new CassandraHandlerDS(DBP);
		try {
			
			// ----------------------------------------------------
			// connect to the data base by providing the database properties
			// ----------------------------------------------------
			Utils.write2Console("CONNECTING TO THE DATABASE.........");
			ch.connect2DB();
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// DROP the table
			// ----------------------------------------------------
			Utils.write2Console("DROPPING TABLE >>> " + sTableName + " <<<");
			ch.processDDLData("DROP TABLE IF EXISTS " + sTableName);
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// CREATE the table
			// ----------------------------------------------------
			Utils.write2Console("CREATING TABLE >>> " + sTableName + " <<<");
			ch.processDMLData("CREATE TABLE " + sTableName + "(userid int, firstname text, lastname text, age int, primary key (userid))");
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// delete a row
			// ----------------------------------------------------
			Utils.write2Console("DELETING THE ROWS FROM >>> " + sTableName + " <<<");
			ch.processDMLData("DELETE FROM " + sTableName + " where userid in (0, 5000)");
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// create the rows
			// ----------------------------------------------------
			Utils.write2Console("CREATING THE ROWS (65535) IN >>> " + sTableName + " <<<");
			BatchStatement batch = new BatchStatement();
			
			ch.setPS("INSERT INTO TEST_RUN_TABLE (userid, firstname, lastname, age) VALUES (?, ?, ?, ?);");
			for (int k = 0; k < 65534; k++) {
				batch.add(ch.getPS().bind(k, "abc", "def", k));
			}
			
			ch.processDMLBatchData(batch);
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// create the rows
			// ----------------------------------------------------
			Utils.write2Console("CREATING THE ROWS (65535 to 131070) IN >>> " + sTableName + " <<<");
			ArrayList alInsert = new ArrayList();
			for (int k = 65535; k < 131070; k++) {
				alInsert.add("INSERT INTO " + sTableName + " (userid, firstname, lastname, age) VALUES (" + k + ", 'abc', 'def'," + k + ")");
			}
			ch.processDMLBatchData(alInsert);
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			alInsert = null;
			
			// ----------------------------------------------------
			// update the rows
			// ----------------------------------------------------
			Utils.write2Console("UPDATING THE ROWS IN >>> " + sTableName + " <<<");
			ch.processDMLData("UPDATE " + sTableName + " SET firstname = 'test123' WHERE userid in (0, 5000)");
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// ----------------------------------------------------
			// [METHOD 1] read the rows
			// ----------------------------------------------------
			int iRowCount = 0;
			Utils.write2Console("(METHOD 1) READING THE comma separated ROWS (65535) FROM >>> " + sTableName + " <<<");
			com.datastax.driver.core.ResultSet rs = ch.exQuery("select * from " + sTableName);
			for (Row row : rs) {
				// reading
				iRowCount += 1;
			}
			Utils.write2Console("(METHOD 1) READ " + iRowCount + " rows");
			
			// ----------------------------------------------------
			// [METHOD 2] read the rows
			// ----------------------------------------------------
			iRowCount = 0;
			Utils.write2Console("(METHOD 2) READING THE DataValue structure ROWS (65535) FROM >>> " + sTableName + " <<<");
			ArrayList alTable = ch.getData("select * from " + sTableName);
			if (ch.isHasError()) {
				throw new Exception(ch.getErrorMessage());
			}
			
			// if the query was successful then CassandraHandler's alData should
			// have the data populated
			Utils.write2Console("(METHOD 2) ITERATING THE DataValue structure<<<");
			if (!alTable.isEmpty()) {
				int alSize = alTable.size();
				for (int i = 0; i < alSize; i++) {
					ArrayList alData = (ArrayList) ch.getAlData().get(i);
					int adSize = alData.size();
					for (int j = 0; j < adSize; j++) {
						DataValue dv = (DataValue) alData.get(j);
						// System.out.print(dv.getColumnFieldName() + "|" + dv.getColumnFieldType()
						// + "|" + dv.getColumnFieldValue());
						// System.out.print(" ; ");
					}
					// System.out.println(" ");
					alData = null;
					iRowCount += 1;
				}
			}
			Utils.write2Console("(METHOD 2) READ " + iRowCount + " rows");
			
			// ----------------------------------------------------
			// close the connection
			// ----------------------------------------------------
			Utils.write2Console("CLOSING THE DATABASE CONNECTION........");
			ch.closeConnection();
			ch = null;
		} catch (Exception e) {
			e.printStackTrace();
			try {
				ch.closeConnection();
			} catch (Exception x) {
				
			}
		}
	}
	
}
