 hdfs dfs -chown -R hbase:hbase hdfs:///user/data/  
 hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs:///user/data/hfiles/d=2017-02-09/test-table_39ac2921-1649-4deb-8fe4-b87f68bbfdd2 test-table
