package com.sb.db.cassandra;

import java.util.ArrayList;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

/**
 * This class is used if a datastax driver is involved.
 * The dbproperties class is mandatory for the constructor of this class
 * Requires: datastax driver (cassandra-java-driver-2.1.8.tar.gz)
 * 
 * @author bizz.sand
 */
public class CassandraHandlerDS {
	// ---------------------------------------------------------------------------------------
	// variables global to the class
	// ---------------------------------------------------------------------------------------
	private static final String				sClassName		= "CassandraHandlerDS";
	private DBProperties					dbp				= null;
	private String							errorMessage	= "";
	private boolean							hasError		= false;
	private ArrayList<ArrayList<DataValue>>	alData			= null;					
	private Cluster							cluster			= null;
	private Session							session			= null;
	private ResultSet						rs				= null;
	private PreparedStatement				ps				= null;
	
	/**
	 * @param DBP
	 *            DBProperties
	 */
	public CassandraHandlerDS(DBProperties DBP) {
		dbp = DBP;
	}
	
	/**
	 * connect to the database and create a session
	 */
	public void connect2DB() {
		// --------------------
		// initialise
		// --------------------
		hasError = false;
		this.errorMessage = "";
		
		try {
			
			// create the session connection
			if (dbp.isLoadBalancedRoundRobinPolicy()) {
				cluster = Cluster
						.builder()
						.addContactPointsWithPorts(dbp.getAddresses())
						.withCredentials(dbp.getUsername(), dbp.getPassword())
						.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
						.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
						.build();
				
			} else if (dbp.isRetryPolicy()) {
				cluster = Cluster
						.builder()
						.addContactPointsWithPorts(dbp.getAddresses())
						.withCredentials(dbp.getUsername(), dbp.getPassword())
						.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
						.build();
			} else {
				cluster = Cluster
						.builder()
						.addContactPointsWithPorts(dbp.getAddresses())
						.withCredentials(dbp.getUsername(), dbp.getPassword())
						.build();
			}
			
			session = cluster.connect(dbp.getKeyspace());
			
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
			ps = null;
			session.close();
			cluster.close();
			session = null;
			cluster = null;
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
	 * change the keyspace
	 * 
	 * @param keyspaceName
	 *            String
	 */
	public void changeKeyspace(String keyspaceName) {
		// --------------------
		// initialise
		// --------------------
		hasError = false;
		this.errorMessage = "";
		try {
			session = cluster.connect(keyspaceName);
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".changeKeyspace\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
		} finally {
			//
		}
	}
	
	/**
	 * process a data definition language statement
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
			session.execute(sDDLString);
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".processDDLData\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
		} finally {
			//
		}
	}
	
	/**
	 * get the data with the column definition
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
			
			// fill in the ArrayList
			ColumnDefinitions cd = rs.getColumnDefinitions();
			int iColCount = cd.size();
			
			for (Row row : rs) {
				alColData = new ArrayList<DataValue>();
				
				for (int iColIndex = 0; iColIndex < iColCount; iColIndex++) {
					// fill in the data
					dv = new DataValue();
					dv.setColumnFieldName(cd.getName(iColIndex));
					dv.setColumnFieldType(cd.getType(iColIndex).toString());
					
					if (dv.getColumnFieldType().equalsIgnoreCase("ascii")) {
						dv.setColumnFieldValue(row.getString(iColIndex).toString());
					} else if (dv.getColumnFieldType().equalsIgnoreCase("bigint")) {
						dv.setColumnFieldValue(row.getInt(iColIndex) + "");
					} else if (dv.getColumnFieldType().equalsIgnoreCase("blob")) {
						dv.setColumnFieldValue("<blob>");
					} else if (dv.getColumnFieldType().equalsIgnoreCase("boolean")) {
						dv.setColumnFieldValue(row.getBool(iColIndex) + "");
					} else if (dv.getColumnFieldType().equalsIgnoreCase("counter")) {
						dv.setColumnFieldValue(row.getInt(iColIndex) + "");
					} else if (dv.getColumnFieldType().equalsIgnoreCase("decimal")) {
						dv.setColumnFieldValue(row.getDecimal(iColIndex).toString());
					} else if (dv.getColumnFieldType().equalsIgnoreCase("double")) {
						dv.setColumnFieldValue(row.getDouble(iColIndex) + "");
					} else if (dv.getColumnFieldType().equalsIgnoreCase("float")) {
						dv.setColumnFieldValue(row.getFloat(iColIndex) + "");
					} else if (dv.getColumnFieldType().equalsIgnoreCase("inet")) {
						dv.setColumnFieldValue(row.getInet(iColIndex).toString());
					} else if (dv.getColumnFieldType().equalsIgnoreCase("int")) {
						dv.setColumnFieldValue(row.getInt(iColIndex) + "");
					} else if (dv.getColumnFieldType().equalsIgnoreCase("list")) {
						dv.setColumnFieldValue("<list>");
					} else if (dv.getColumnFieldType().equalsIgnoreCase("map")) {
						dv.setColumnFieldValue("<map>");
					} else if (dv.getColumnFieldType().equalsIgnoreCase("set")) {
						dv.setColumnFieldValue("<set>");
					} else if (dv.getColumnFieldType().equalsIgnoreCase("text")) {
						dv.setColumnFieldValue(row.getString(iColIndex));
					} else if (dv.getColumnFieldType().equalsIgnoreCase("timestamp")) {
						dv.setColumnFieldValue(row.getString(iColIndex));
					} else if (dv.getColumnFieldType().equalsIgnoreCase("uuid")) {
						dv.setColumnFieldValue(row.getUUID(iColIndex).toString());
					} else if (dv.getColumnFieldType().equalsIgnoreCase("timeuuid")) {
						dv.setColumnFieldValue(row.getUUID(iColIndex).toString());
					} else if (dv.getColumnFieldType().equalsIgnoreCase("varchar")) {
						dv.setColumnFieldValue(row.getString(iColIndex));
					} else if (dv.getColumnFieldType().equalsIgnoreCase("varint")) {
						dv.setColumnFieldValue(row.getInt(iColIndex) + "");
					}
					
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
	 * process a data modification statement
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
			session.execute(sDMLString);
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".processDMLData\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
		} finally {
			//
		}
	}
	
	/**
	 * process data modification statement in a batch. maximum 65535
	 * 
	 * @param batch
	 *            BatchStatement
	 */
	public void processDMLBatchData(BatchStatement batch) {
		// --------------------
		// initialise
		// --------------------
		hasError = false;
		this.errorMessage = "";
		
		try {
			session.execute(batch);
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".processDMLBatchData\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
		} finally {
			//
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
			
			session.execute(sbQUERY);
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".processDMLBatchData\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			// --------------------
		} finally {
			//
		}
	}
	
	/**
	 * executes a query and returns the resultset
	 * 
	 * @param sQuery
	 *            String
	 * @return ResultSet
	 */
	public ResultSet exQuery(String sQuery) {
		rs = null;
		hasError = false;
		try {
			rs = session.execute(sQuery);
		} catch (Exception e) {
			// --------------------
			// the be-spoked message
			// --------------------
			this.errorMessage = sClassName + ".exQuery\n" + e.getMessage();
			e.printStackTrace();
			hasError = true;
			rs = null;
			// --------------------
		} finally {
			//
		}
		
		return rs;
	}
	
	// ----------------------------------------------------------------------------------------
	// all getters and setters below this line
	// ----------------------------------------------------------------------------------------
	
	/**
	 * @return the errorMessage
	 */
	public String getErrorMessage() {
		return errorMessage;
	}
	
	/**
	 * @param errorMessage
	 *            the errorMessage to set
	 */
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
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
	
	/**
	 * @return the cluster
	 */
	public Cluster getCluster() {
		return cluster;
	}
	
	/**
	 * @return the session
	 */
	public Session getSession() {
		return session;
	}
	
	/**
	 * @return the rs
	 */
	public ResultSet getRs() {
		return rs;
	}
	
	/**
	 * @return the ps
	 */
	public PreparedStatement getPS() {
		return ps;
	}
	
	/**
	 * @param sSQL
	 *            String
	 */
	public void setPS(String sSQL) {
		ps = session.prepare(sSQL);
	}
}
