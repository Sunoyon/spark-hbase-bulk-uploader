package org.hiyam.dci.hbase

import java.util.UUID

import scala.collection.JavaConversions.asScalaSet
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.RegionLocator
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.storage.StorageLevel

import org.hiyam.hbase.utils.Constants

object BulkUploader {

  private object HFilePartitioner {
    def apply(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegionPerFamily: Int) = {
      if (numFilesPerRegionPerFamily == 1)
        new SingleHFilePartitioner(splits)
      else {
        val fraction = 1 max numFilesPerRegionPerFamily min conf.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)
        new MultiHFilePartitioner(splits, fraction)
      }
    }
  }

  protected abstract class HFilePartitioner extends Partitioner {
    def extractKey(n: Any) = n match {
      case (k: Array[Byte], _) => k
      case ((k: Array[Byte], _), _) => k
    }
  }

  private class MultiHFilePartitioner(splits: Array[Array[Byte]], fraction: Int) extends HFilePartitioner {
    override def getPartition(key: Any): Int = {
      val k = extractKey(key)
      val h = (k.hashCode() & Int.MaxValue) % fraction
      for (i <- 1 until splits.length)
        if (Bytes.compareTo(k, splits(i)) < 0) return (i - 1) * fraction + h

      (splits.length - 1) * fraction + h
    }

    override def numPartitions: Int = splits.length * fraction
  }

  private class SingleHFilePartitioner(splits: Array[Array[Byte]]) extends HFilePartitioner {
    override def getPartition(key: Any): Int = {
      val k = extractKey(key)
      for (i <- 1 until splits.length)
        if (Bytes.compareTo(k, splits(i)) < 0) return i - 1

      splits.length - 1
    }

    override def numPartitions: Int = splits.length
  }

  protected def getPartitioner(conf: Configuration, regionLocator: RegionLocator, numFilesPerRegionPerFamily: Int) =
    HFilePartitioner(conf, regionLocator.getStartKeys, numFilesPerRegionPerFamily)

  protected def getPartitionedRdd[C: ClassTag, A: ClassTag](rdd: RDD[(C, A)], family: String, partitioner: HFilePartitioner)(implicit ord: Ordering[C]) = {
    rdd
      .repartitionAndSortWithinPartitions(partitioner)
      .map { case (cell: ((Array[Byte], Array[Byte]), Long), value: Array[Byte]) => (new ImmutableBytesWritable(cell._1._1), new KeyValue(cell._1._1, Bytes.toBytes(family), cell._1._2, cell._2, value)) }
  }

  protected def saveAsHFile(rdd: RDD[(ImmutableBytesWritable, KeyValue)], conf: Configuration, table: Table, regionLocator: RegionLocator, connection: Connection, hPath: String) = {
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))

    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

    // prepare path for HFiles output
    val fs = FileSystem.get(conf)
    val hFilePath = new Path(hPath, table.getName.getQualifierAsString + "_" + UUID.randomUUID())
    fs.makeQualified(hFilePath)

    try {
      rdd.saveAsNewAPIHadoopFile(hFilePath.toString(), classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
      val lih = new LoadIncrementalHFiles(conf)
      lih.doBulkLoad(hFilePath, connection.getAdmin, table, regionLocator)
    } finally {
      connection.close()
    }
  }

  implicit val ordForBytes = new math.Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = {
      Bytes.compareTo(a, b)
    }
  }

  implicit val ordForLong = new math.Ordering[Long] {
    def compare(a: Long, b: Long): Int = {
      if (a < b) {
        1
      } else if (a > b) {
        -1
      } else {
        0
      }
    }
  }

  def toHBaseBulk(mapRdd: RDD[(Array[Byte], Map[String, Array[(String, (String, Long))]])], conf: Configuration, tableNameStr: String, numFilesPerRegionPerFamily: Int = 1, hFilePath : String) = {
    mapRdd.persist(StorageLevel.DISK_ONLY)

    require(numFilesPerRegionPerFamily > 0)

    val tableName = TableName.valueOf(tableNameStr)
    val connection = ConnectionFactory.createConnection(conf)
    val regionLocator = connection.getRegionLocator(tableName)
    val table = connection.getTable(tableName)

    val families = table.getTableDescriptor.getFamiliesKeys map Bytes.toString
    val partitioner = getPartitioner(conf, regionLocator, numFilesPerRegionPerFamily)

    val rdds = for {
      f <- families
      rdd = mapRdd
        .collect { case (k, m) if m.contains(f) => (k, m(f)) }
        .flatMap {
          case (k, m) =>
            m map { case (h: String, v: (String, Long)) => (((k, Bytes.toBytes(h)), v._2), Bytes.toBytes(v._1)) }
        }
    } yield getPartitionedRdd(rdd, f, partitioner)

    saveAsHFile(rdds.reduce(_ ++ _), conf, table, regionLocator, connection, hFilePath)

    mapRdd.unpersist()
  }
}
