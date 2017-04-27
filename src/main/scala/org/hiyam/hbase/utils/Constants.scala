package org.hiyam.hbase.utils

import org.apache.spark.SparkContext

object Constants {

  val HBASE_HFILE_DIRECTORY = "/user/data/hfiles/"
  val COLUMN_QUALIFIER_TEXT = "TEXT"

  def sparkHadoopConf(sc: SparkContext) = {
    sc.hadoopConfiguration.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.hadoopConfiguration.set("fs.s3.canned.acl", "BucketOwnerFullControl")
  }
}