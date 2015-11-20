package com.sb.db.cassandra;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * This class is used to set the database required properties. 
 * It is a common class for setting up jdbc or ds properties (refer TestCassandraDB for implementation)
 * 
 * @author bizz.sand
 */

public class DBProperties {
	// driver properties
	private String							jdbcDriver						= "org.apache.cassandra.cql.jdbc.CassandraDriver";
	private String							className						= "com.lp.db.cassandradb.CassandraHandler";
	
	// db connectivity properties
	private String							username						= "";
	private String							password						= "";
	private String							keyspace						= "";
	private String							node							= "";
	private String							port							= "";
	private boolean							cassandraJDBCDriver				= false;
	private boolean							cassandraDSDriver				= false;
	private boolean							retryPolicy						= true;
	private boolean							loadBalancedRoundRobinPolicy	= true;
	private Collection<InetSocketAddress>	addresses						= null;
	
	/**
	 * @return the jdbcDriver
	 */
	public String getJdbcDriver() {
		return jdbcDriver;
	}
	
	/**
	 * @param jdbcDriver
	 *            the jdbcDriver to set
	 */
	public void setJdbcDriver(String jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}
	
	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}
	
	/**
	 * @param username
	 *            the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}
	
	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}
	
	/**
	 * @param password
	 *            the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}
	
	/**
	 * @return the keyspace
	 */
	public String getKeyspace() {
		return keyspace;
	}
	
	/**
	 * @param keyspace
	 *            the keyspace to set
	 */
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}
	
	/**
	 * @return the node
	 */
	public String getNode() {
		return node;
	}
	
	/**
	 * @param node
	 *            the node to set
	 */
	public void setNode(String node) {
		this.node = node;
	}
	
	/**
	 * @return the port
	 */
	public String getPort() {
		return port;
	}
	
	/**
	 * @param port
	 *            the port to set
	 */
	public void setPort(String port) {
		this.port = port;
	}
	
	/**
	 * @return the cassandraJDBCDriver
	 */
	public boolean isCassandraJDBCDriver() {
		return cassandraJDBCDriver;
	}
	
	/**
	 * @param cassandraJDBCDriver
	 *            the cassandraJDBCDriver to set
	 */
	public void setCassandraJDBCDriver(boolean cassandraJDBCDriver) {
		this.cassandraJDBCDriver = cassandraJDBCDriver;
	}
	
	/**
	 * @return the cassandraDSDriver
	 */
	public boolean isCassandraDSDriver() {
		return cassandraDSDriver;
	}
	
	/**
	 * @param cassandraDSDriver
	 *            the cassandraDSDriver to set
	 */
	public void setCassandraDSDriver(boolean cassandraDSDriver) {
		this.cassandraDSDriver = cassandraDSDriver;
	}
	
	/**
	 * @return the className
	 */
	public String getClassName() {
		return className;
	}
	
	/**
	 * @param className
	 *            the className to set
	 */
	public void setClassName(String className) {
		this.className = className;
	}
	
	/**
	 * @return the retryPolicy
	 */
	public boolean isRetryPolicy() {
		return retryPolicy;
	}
	
	/**
	 * @param retryPolicy
	 *            the retryPolicy to set
	 */
	public void setRetryPolicy(boolean retryPolicy) {
		this.retryPolicy = retryPolicy;
	}
	
	/**
	 * @return the loadBalancedRoundRobinPolicy
	 */
	public boolean isLoadBalancedRoundRobinPolicy() {
		return loadBalancedRoundRobinPolicy;
	}
	
	/**
	 * @param loadBalancedRoundRobinPolicy
	 *            the loadBalancedRoundRobinPolicy to set
	 */
	public void setLoadBalancedRoundRobinPolicy(boolean loadBalancedRoundRobinPolicy) {
		this.loadBalancedRoundRobinPolicy = loadBalancedRoundRobinPolicy;
	}
	
	/**
	 * @return the addresses
	 */
	public Collection<InetSocketAddress> getAddresses() {
		return addresses;
	}
	
	/**
	 * @param addresses
	 *            the addresses to set
	 */
	public void setAddresses(Collection<InetSocketAddress> addresses) {
		this.addresses = addresses;
	}
}
