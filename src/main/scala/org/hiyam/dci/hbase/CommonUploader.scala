package org.hiyam.dci.hbase

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.hiyam.hbase.utils.Constants
import org.hiyam.dci.hbase.config.parser.ConfigParser
import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.hiyam.dci.hbase.config.parser.PathParamParser
import org.apache.hadoop.hbase.HBaseConfiguration
import scala.collection.mutable.HashMap
import org.hiyam.dci.hbase.utils.SystemUtils
import scala.collection.mutable.LinkedHashMap
import org.apache.spark.sql.SparkSession

object CommonUploader {
  def readData(sqlContext: SQLContext, schema: StructType, path: String, isHeader: Boolean, delimiter: String, isZip: Boolean): DataFrame = {
    if (isZip) {
      sqlContext.read.format("com.databricks.spark.csv").option("header", String.valueOf(isHeader)).option("delimiter", delimiter).option("codec", "org.apache.hadoop.io.compress.GzipCodec").schema(schema).load(path)
    } else {
      sqlContext.read.format("com.databricks.spark.csv").option("header", String.valueOf(isHeader)).option("delimiter", delimiter).schema(schema).load(path)
    }
  }

  def getSchema(columns: List[String]): StructType = {
    if (columns.isEmpty) {
      null
    }
    val structFields = ListBuffer[StructField]()
    for (col <- columns) structFields += StructField(col, StringType, true)
    StructType(structFields)
  }

  def getRows(df: DataFrame, columnFamily: String, key: String, columnQualifiers: LinkedHashMap[String, String], valueMap: (String, String), separator: String): RDD[(Array[Byte], Map[String, Array[(String, (String, Long))]])] = {
    df.rdd.map(r => {
      (r.getAs[String](key), r)
    }).groupByKey().map(r => {
      val rowKey = r._1
      val rowValues = r._2

      var cells = new HashMap[String, Array[(String, (String, Long))]]
      var columnQualifier = ArrayBuffer[(String, (String, Long))]()
      for (rowValue <- rowValues) {
        var qualifierParts = ListBuffer[String]()
        for ((k, v) <- columnQualifiers) qualifierParts += (if (Constants.COLUMN_QUALIFIER_TEXT == v) k else rowValue.getAs[String](k))

        val qualifier = qualifierParts.mkString(separator)
        val value = if (valueMap._2 == Constants.COLUMN_QUALIFIER_TEXT) valueMap._1 else rowValue.getAs[String](valueMap._1)
        columnQualifier += ((qualifier, (value, System.currentTimeMillis())))
      }
      cells.put(columnFamily, columnQualifier.toArray)
      (Bytes.toBytes(rowKey), cells.toMap)
    })
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      print("YAML configuration file is missing.")
      System.exit(1);
    }
    val configFile = args(0)
    val config = ConfigParser.getInstance.getTaskConfig(configFile)
    val schema = getSchema(config.getS3ColumnsInOrder().asScala.toList)
    val dataDir = PathParamParser.getS3FullPath(config.getS3BaseLocation(), config.getS3PathParam())

    val conf = new SparkConf().setAppName("Hbase-Bulk-Uploader")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    var hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", config.getHbaseHost())

    val data = readData(sqlContext, schema, dataDir, config.getIsColumnHeader(), config.getDelimiter(), true)
    for (cf <- config.getHbaseColumnFamilies().asScala.toList) {
      val colQual = LinkedHashMap(cf.getColumnQualifiersInOrder().asScala.toSeq: _*)
      val valMap = cf.getValueColumn().asScala.head
      val hbaseRows = getRows(data, cf.getName(), config.getKeyColumn(), colQual, valMap, config.getHbaseColumnQualifierSeparator())
      val dest = PathParamParser.getS3FullPath(Constants.HBASE_HFILE_DIRECTORY, cf.getS3DestPathParam())
      BulkUploader.toHBaseBulk(hbaseRows, hbaseConf, config.getHbaseTableName, cf.getHfilePerRegion, dest)
    }
    System.exit(0)
  }
}