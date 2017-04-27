package org.hiyam.dci.hbase.config;

import java.util.List;
import java.util.Map;

public class Task {

	private String s3BaseLocation;
	private List<Map<String, String>> s3PathParam;
	private List<String> s3ColumnsInOrder;
	private String keyColumn;
	private String hbaseHost;
	private String hbaseTableName;
	private String hbaseColumnQualifierSeparator;
	private List<ColumnFamilyConfig> hbaseColumnFamilies;
	private String delimiter;

	public Task() {
	}

	public Task(String s3BaseLocation, List<Map<String, String>> s3PathParam, List<String> s3ColumnsInOrder, String keyColumn,
			String hbaseHost, String hbaseTableName, String hbaseColumnQualifierSeparator,
			List<ColumnFamilyConfig> hbaseColumnFamilies, String delimiter) {
		this.s3BaseLocation = s3BaseLocation;
		this.s3PathParam = s3PathParam;
		this.s3ColumnsInOrder = s3ColumnsInOrder;
		this.keyColumn = keyColumn;
		this.hbaseHost = hbaseHost;
		this.hbaseTableName = hbaseTableName;
		this.hbaseColumnQualifierSeparator = hbaseColumnQualifierSeparator;
		this.hbaseColumnFamilies = hbaseColumnFamilies;
		this.delimiter = delimiter;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getS3BaseLocation() {
		return s3BaseLocation;
	}

	public void setS3BaseLocation(String s3BaseLocation) {
		this.s3BaseLocation = s3BaseLocation;
	}

	public List<Map<String, String>> getS3PathParam() {
		return s3PathParam;
	}

	public void setS3PathParam(List<Map<String, String>> s3PathParam) {
		this.s3PathParam = s3PathParam;
	}

	public List<String> getS3ColumnsInOrder() {
		return s3ColumnsInOrder;
	}

	public void setS3ColumnsInOrder(List<String> s3ColumnsInOrder) {
		this.s3ColumnsInOrder = s3ColumnsInOrder;
	}

	public String getKeyColumn() {
		return keyColumn;
	}

	public void setKeyColumn(String keyColumn) {
		this.keyColumn = keyColumn;
	}

	public String getHbaseHost() {
		return hbaseHost;
	}

	public void setHbaseHost(String hbaseHost) {
		this.hbaseHost = hbaseHost;
	}

	public String getHbaseTableName() {
		return hbaseTableName;
	}

	public void setHbaseTableName(String hbaseTableName) {
		this.hbaseTableName = hbaseTableName;
	}

	public String getHbaseColumnQualifierSeparator() {
		return hbaseColumnQualifierSeparator;
	}

	public void setHbaseColumnQualifierSeparator(String hbaseColumnQualifierSeparator) {
		this.hbaseColumnQualifierSeparator = hbaseColumnQualifierSeparator;
	}

	public List<ColumnFamilyConfig> getHbaseColumnFamilies() {
		return hbaseColumnFamilies;
	}

	public void setHbaseColumnFamilies(List<ColumnFamilyConfig> hbaseColumnFamilies) {
		this.hbaseColumnFamilies = hbaseColumnFamilies;
	}

}
