package com.sb.db.cassandra;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * This class is used if a jdbc driver is involved.
 * The dbproperties class is mandatory for the constructor of this class
 * 
 * Requires: cassandra-jdbc-1.2.5.jar and cassandra-jdbc-dependencies
 *
 * @author bizz.sand
 */
public class CassandraHandlerJDBC {
	
	// ---------------------------------------------------------------------------------------
	// variables global to the class
	// ---------------------------------------------------------------------------------------
	private static final String				sClassName		= "CassandraHandlerJDBC";
	private Connection						connection		= null;
	private Statement						statement		= null;
	private ResultSet						resultSet		= null;
	private ResultSetMetaData				resultMetaData	= null;
	private DBProperties					dbp				= null;
	private String							errorMessage	= "";
	private boolean							hasError		= false;
	private ArrayList<ArrayList<DataValue>>	alData			= null;	
	
	/**
	 * constructor
	 * 
	 * @param DBP
	 *            DBPRoperties
	 */
	public CassandraHandlerJDBC(DBProperties DBP) {
		dbp = DBP;
	}
	
	/**
	 * connect to the db using jdbc
	 */
	public void connect2DB() {
		// --------------------
		// initialise
		// --------------------
		hasError = false;
		this.errorMessage = "";
		
		try {
			// load the driver
			Class.forName(dbp.getJdbcDriver());
			
			// create the connection
			connection = DriverManager.getConnection("jdbc:cassandra://" + dbp.getNode() + ":" + dbp.getPort() + "/" + dbp.getKeyspace(), dbp.getUsername(), dbp.getPassword());
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".connect2DB\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
			
			// cleanse
			closeConnection();
		}
	}
	
	/**
	 * close the connection
	 */
	public void closeConnection() {
		// --------------------
		// initialise
		// --------------------
		hasError = false;
		this.errorMessage = "";
		
		try {
			connection.close();
			connection = null;
			resultSet = null;
			resultMetaData = null;
			statement = null;
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".closeConnection\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
		}
	}
	
	/**
	 * process the data definition statement
	 * 
	 * @param sDDLString
	 *            String
	 */
	public void processDDLData(String sDDLString) {
		// --------------------
		// initialise
		// --------------------
		hasError = false;
		this.errorMessage = "";
		try {
			statement = connection.createStatement();
			statement.execute(sDDLString);
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".processDDLData\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
		} finally {
			statement = null;
		}
	}
	
	/**
	 * gets the data with the column name and type
	 * 
	 * @param sQuery
	 *            String
	 * @return ArrayList containing an ArrayList of DataValue
	 */
	public ArrayList<ArrayList<DataValue>> getData(String sQuery) {
		// --------------------
		// initialise
		// --------------------
		hasError = false;
		this.errorMessage = "";
		
		this.alData = new ArrayList<ArrayList<DataValue>>(); // to hold the rows --> alColData
		ArrayList<DataValue> alColData = null; // to hold the column data --> DataValue
		DataValue dv; // to hold the column type, name, value
		
		try {
			// get the data
			exQuery(sQuery);
			
			// get the resultMetaData
			resultMetaData = resultSet.getMetaData();
			
			// fill in the ArrayList
			int iColCount = resultMetaData.getColumnCount();
			while (resultSet.next()) {
				alColData = new ArrayList<DataValue>();
				for (int iColIndex = 1; iColIndex <= iColCount; iColIndex++) {
					// fill in the data
					dv = new DataValue();
					dv.setColumnFieldName(resultMetaData.getColumnName(iColIndex));
					dv.setColumnFieldType(resultMetaData.getColumnTypeName(iColIndex));
					dv.setColumnFieldValue(resultSet.getString(iColIndex));
					
					// add the col data
					alColData.add(dv);
				}
				// add the row
				alData.add(alColData);
				alColData = null;
			}
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".getData\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
			
			alColData = null;
			dv = null;
		}
		return alData;
	}
	
	/**
	 * process data manipulation statement
	 * 
	 * @param sDMLString
	 *            String
	 */
	public void processDMLData(String sDMLString) {
		// --------------------
		// initialise
		// --------------------
		hasError = false;
		this.errorMessage = "";
		try {
			statement = connection.createStatement();
			statement.executeUpdate(sDMLString);
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".processDMLData\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
		} finally {
			statement = null;
		}
	}
	
	/**
	 * process data manipulation statement in a batch
	 * 
	 * @param alDMLString
	 *            String
	 */
	public void processDMLBatchData(ArrayList<String> alDMLString) {
		// --------------------
		// initialise
		// --------------------
		hasError = false;
		this.errorMessage = "";
		
		String sbQUERY = "BEGIN BATCH \n";
		try {
			String sValue = alDMLString.toString();
			sValue = sValue.substring(1, sValue.length() - 1);
			sValue = sValue.replace("),", ")\n");
			
			sbQUERY = sbQUERY + sValue + "\n APPLY BATCH";
			
			statement = connection.createStatement();
			statement.executeUpdate(sbQUERY);
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".processDMLBatchData\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
		} finally {
			statement = null;
		}
	}
	
	/**
	 * execute a query string
	 * 
	 * @param sQuery
	 *            String
	 * @return ResultSet
	 */
	public ResultSet exQuery(String sQuery) {
		resultSet = null;
		hasError = false;
		try {
			// create the statement
			statement = connection.createStatement();
			// execute the query
			resultSet = statement.executeQuery(sQuery);
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".processBatchData\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			resultSet = null;
			// --------------------
		} finally {
			statement = null;
		}
		
		return resultSet;
	}
	
	// ----------------------------------------------------------------------------------------
	// all getters and setters below this line
	// ----------------------------------------------------------------------------------------
	
	/**
	 * @return the connection
	 */
	public Connection getConnection() {
		return connection;
	}
	
	/**
	 * @return the resultSet
	 */
	public ResultSet getResultSet() {
		return resultSet;
	}
	
	/**
	 * @return the errorMessage
	 */
	public String getErrorMessage() {
		return errorMessage;
	}
	
	/**
	 * @return the hasError
	 */
	public boolean isHasError() {
		return hasError;
	}
	
	/**
	 * @return the alData
	 */
	public ArrayList<ArrayList<DataValue>> getAlData() {
		return alData;
	}
	
}
