package org.hiyam.dci.hbase.config;

import java.util.List;
import java.util.Map;

public class ColumnFamilyConfig {

	private String name;
	private Map<String, String> columnQualifiersInOrder;
	private Map<String, String> valueColumn;
	private Integer hfilePerRegion;
	private String s3DestBaseLocation;
	private List<Map<String, String>> s3DestPathParam;

	public ColumnFamilyConfig() {
	}

	public ColumnFamilyConfig(String name, Map<String, String> columnQualifiersInOrder, Map<String, String> valueColumn,
			Integer hfilePerRegion, String s3DestBaseLocation, List<Map<String, String>> s3DestPathParam) {
		super();
		this.name = name;
		this.columnQualifiersInOrder = columnQualifiersInOrder;
		this.valueColumn = valueColumn;
		this.hfilePerRegion = hfilePerRegion;
		this.s3DestBaseLocation = s3DestBaseLocation;
		this.s3DestPathParam = s3DestPathParam;
	}

	public String getS3DestBaseLocation() {
		return s3DestBaseLocation;
	}

	public void setS3DestBaseLocation(String s3DestBaseLocation) {
		this.s3DestBaseLocation = s3DestBaseLocation;
	}

	public List<Map<String, String>> getS3DestPathParam() {
		return s3DestPathParam;
	}

	public void setS3DestPathParam(List<Map<String, String>> s3DestPathParam) {
		this.s3DestPathParam = s3DestPathParam;
	}

	public Integer getHfilePerRegion() {
		return hfilePerRegion;
	}

	public void setHfilePerRegion(Integer hfilePerRegion) {
		this.hfilePerRegion = hfilePerRegion;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, String> getColumnQualifiersInOrder() {
		return columnQualifiersInOrder;
	}

	public void setColumnQualifiersInOrder(Map<String, String> columnQualifiersInOrder) {
		this.columnQualifiersInOrder = columnQualifiersInOrder;
	}

	public Map<String, String> getValueColumn() {
		return valueColumn;
	}

	public void setValueColumn(Map<String, String> valueColumn) {
		this.valueColumn = valueColumn;
	}

}
