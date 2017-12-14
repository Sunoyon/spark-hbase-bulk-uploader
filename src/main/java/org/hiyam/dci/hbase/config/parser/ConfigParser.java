package org.hiyam.dci.hbase.config.parser;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hiyam.dci.hbase.config.ColumnFamilyConfig;
import org.hiyam.dci.hbase.config.Task;
import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigParser {

	private static final ConfigParser instance = new ConfigParser();

	private ConfigParser() {

	}

	public static ConfigParser getInstance() {
		return instance;
	}

	@SuppressWarnings("unchecked")
	public Task getTaskConfig(String configFile) throws Exception {
		File f = new File(configFile);
		if (!f.exists() || f.isDirectory()) {
			throw new Exception("Config file Not found");
		}
		Yaml yaml = new Yaml();
		Map<String, Object> yamlParsers = (Map<String, Object>) yaml.load(new FileInputStream(f));
		List<Map<String, Object>> hbaseCf = (List<Map<String, Object>>) yamlParsers.get("hbase_column_families");

		List<ColumnFamilyConfig> cfConfig = new ArrayList<ColumnFamilyConfig>();
		ObjectMapper mapper = new ObjectMapper();
		for (Map<String, Object> cf : hbaseCf) {
			List<Map<String, String>> destPathParam = mapper.readValue((String) cf.get("s3_dest_path_param_json"),
					new TypeReference<List<Map<String, String>>>() {
					});
			cfConfig.add(new ColumnFamilyConfig((String) cf.get("name"),
					(Map<String, String>) cf.get("column_qualifiers_in_order"),
					(Map<String, String>) cf.get("value_column"), (Integer) cf.get("hfile_per_region"),
					(String) cf.get("s3_dest_base_location"), destPathParam));

		}
		List<Map<String, String>> pathParam = mapper.readValue((String) yamlParsers.get("s3_path_param_json"),
				new TypeReference<List<Map<String, String>>>() {
				});
		Boolean isColumnHeader = Boolean.valueOf(String.valueOf(yamlParsers.get("s3_columns_header")));
		return new Task((String) yamlParsers.get("s3_base_location"), pathParam, isColumnHeader,
				(List<String>) yamlParsers.get("s3_columns_in_order"), (String) yamlParsers.get("key_column"),
				(String) yamlParsers.get("hbase_host"), (String) yamlParsers.get("hbase_table_name"),
				(String) yamlParsers.get("hbase_column_qualifier_separator"), cfConfig,
				(String) yamlParsers.get("s3_data_delimiter"));
	}

}
