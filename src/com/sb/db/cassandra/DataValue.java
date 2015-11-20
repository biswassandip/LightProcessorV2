package com.sb.db.cassandra;

/**
 * This class is just used if you need to represent your data in a key-value pair
 * 
 * @author bizz.sand
 */
public class DataValue
{
	private String	columnFieldName		= "";
	private String	columnFieldType		= "";
	private String	columnFieldValue	= "";
	
	/**
	 * @return the columnFieldName
	 */
	public String getColumnFieldName() {
		return columnFieldName;
	}
	
	/**
	 * @param columnFieldName
	 *            the columnFieldName to set
	 */
	public void setColumnFieldName(String columnFieldName) {
		this.columnFieldName = columnFieldName;
	}
	
	/**
	 * @return the columnFieldType
	 */
	public String getColumnFieldType() {
		return columnFieldType;
	}
	
	/**
	 * @param columnFieldType
	 *            the columnFieldType to set
	 */
	public void setColumnFieldType(String columnFieldType) {
		this.columnFieldType = columnFieldType;
	}
	
	/**
	 * @return the columnFieldValue
	 */
	public String getColumnFieldValue() {
		return columnFieldValue;
	}
	
	/**
	 * @param columnFieldValue
	 *            the columnFieldValue to set
	 */
	public void setColumnFieldValue(String columnFieldValue) {
		this.columnFieldValue = columnFieldValue;
	}
}
