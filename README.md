
# Spark Hbase Bulk Uploader

This is an utility tool to upload data in bulk mode to Hbase using Spark. Currently it can upload tsv and csv. Avro and parquet support will be provided later. 
	
Spark Version: 1.6.2+

Hbase Version: 1.2.2

Spark version can be changed by modifying pom.

## Spark Submit
This spark-submit configuration is for 4 r3.4xlarge of AWS EMR cluster.

	spark-submit \
	--class org.hiyam.dci.hbase.CommonUploader \
	--master yarn --driver-cores 4 --executor-cores 4 \
	--conf "spark.executor.memory=20GB" --driver-memory 20G \
	--packages com.databricks:spark-csv_2.10:1.3.0,com.databricks:spark-avro_2.10:2.0.1 \
	--num-executors 15 \
	--conf "spark.akka.threads=8" \
	--conf "spark.dynamicAllocation.enabled=false" --conf "spark.yarn.submit.file.replication=1" \
	--conf spark.sql.caseSensitive=true \
	--conf spark.network.timeout=36000s \
	--conf spark.default.parallelism=96 \
	--conf "spark.sql.shuffle.partitions=512" \
	spark-hbase-upload-0.0.1-jar-with-dependencies.jar \
	example_config.yaml

## Yaml Configuration Details

**s3_base_location**: Base location of data source. e.g, s3://bucket1/p1/

**s3_path_param_json**: Array of json object. Needed for dynamic path partition. It must be empty string when data exists in the base location. 

e.g, '[{"key": "d", "value": "-d 1 -h 0 -f yyyy-MM-dd", "type": "date"}, {"key": "h", "value": "-d 0 -h 1 -f HH", "type": "date"}, {"key": "p", "value": "market", "type": "text"}]'

Json Object Details:
- key: Key of the path partition.
- value: value of the path partition.
- type: partition type. Currently supported type: 'date' and 'text'.

e.g, 

For "text" type

{"key": "p", "value": "market", "type": "text"} denotes the path partition is "p=market/"

{"key": "", "value": "market", "type": "text"} denotes the path partition is "market/"



For "date" type, value is always calculated from current date time. 

{"key": "d", "value": "-d 1 -h 0 -f yyyy-MM-dd", "type": "date"}

Here, 
	
"-d 1" indicates 1 day before from current day.

"-h 0" indicates 0 hour before current hour.

"-f yyyy-MM-dd" is the date format. 


If current date time is 2017-03-02 05:34:55, {"key": "d", "value": "-d 1 -h 0 -f yyyy-MM-dd", "type": "date"} denotes "d=2017-03-01/"



A complete example: 

If current date time is 2017-03-02 01:34:55,

s3_base_location : s3://bucket1/p1/

s3_path_param_json: '[{"key": "d", "value": "-d 1 -h 0 -f yyyy-MM-dd", "type": "date"}, {"key": "h", "value": "-d 0 -h 1 -f HH", "type": "date"}, {"key": "p", "value": "market", "type": "text"}]'

These two configuration generates s3://bucket1/p1/d=2017-03-01/h=00/p=market/ as the source path.



**s3_data_delimiter**: Delimiter of the source data. e.g, "\\t" or ","

**s3_columns_in_order**: Array of column names (in order) of source data.

e.g,
    - uid
    - categoryId
    - ruleId
    - prob

**key_column**: Source column name for HBASE Key. 

**hbase_host**: Host URL of HBASE master. 

**hbase_table_name**: HBASE table name.

**hbase_column_qualifier_separator**: Separator character of column qualifiers. e.g, "-"

**hbase_column_families**: Array of object for HBASE Column Family configuration.
    
    - **name**: HBASE Column Family name.
      **column_qualifiers_in_order**: Array of HBASE Column Qualifier configuration. 
      e.g, 
          iab: TEXT
          ruleId: VALUE
          categoryId: VALUE
      TEXT indicates text before colon sign is added in Column Qualifier as constant. 
      VALUE indicates data of source column (specified before the colon sign) is added in the Column Qualifier.
      **value_column**: Value field for HBASE. e.g,
          prob: VALUE
      VALUE indicates data of source column (specified before the colon sign) is added in HBASE value field.     
      **hfile_per_region**: Number of hfile partition per region. e.g, 1
      **s3_dest_base_location**: Base location of final hfile. Hfile is stored in file format for future purpose. 
      **s3_dest_path_param_json**: Path partition of final hfile location. It should be same as **s3_path_param_json**.

For example: A row from HBASE of the configuration.
	  ROW                                            COLUMN+CELL                                                                                                                           
 	  1000000005624220581                           column=cf:iab-100-5, timestamp=1492965215857, value=0.6  
1000000005624220581: HBASE Key. ["uid" from source data]
value=0.6: Value Field. ["prob" from source data]
cf: Column Family.
iab-100-5: Column Qualifiers. 
	iab: Constant Text.
	100: "ruleId"  from source data.
	5: "categoryId" from source data.
