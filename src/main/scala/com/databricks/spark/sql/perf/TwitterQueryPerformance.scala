package com.databricks.spark.sql.perf

import java.io.{File, FilenameFilter}

import com.databricks.spark.sql.perf.ExecutionMode.CountResults
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, _}
import org.apache.spark.storage.StorageLevel

import scala.io.Source

/**
 * Created by wuxiaoqi on 17-1-4.
 */
class TwitterQueryPerformance extends Benchmark {

  import sqlContext.implicits._


  val joinTables = Seq(
    Table("edges", sqlContext.sparkSession.read.json(sqlContext.getConf("spark.sql.perf.dataSource")))
  )

  joinTables.map{
    case t =>
      t.data.createOrReplaceTempView(t.name)
      t.data.persist(StorageLevel.MEMORY_AND_DISK)
      println(s"${t.name} num: ${t.data.count()}")
  }
  private val table = sqlContext.table _

  val varyType =
    Seq[Query](
      new Query(
        s"multi-join-count triangles: ",
        table("edges").as("a").join(table("edges").as("b"), $"a.target" === $"b.source")
          .join(table("edges").as("c"), $"b.target" === $"c.source" && $"c.target" === $"a.source")
          .where($"a.source" < $"b.source" && $"b.source" < $"c.source"))
      //      new Query(
      //        s"multi-join-count chains(3 nodes): ",
      //        table("edges").as("a").join(table("edges").as("b"), $"a.target" === $"b.source")
      //          .join(table("edges").as("c"), $"b.target" === $"c.source")
      //          .where($"a.source" < $"b.source" && $"b.source" < $"c.source"))
      //    new Query(
      //      s"multi-join-count clovers: ",
      //      table("edges").as("a").join(table("edges").as("b"), $"a.source" === $"b.source")
      //        .join(table("edges").as("c"), $"a.source" === $"c.source")
      //        .join(table("edges").as("d"), $"a.source" === $"d.source")
      //        .where($"a.target" < $"b.target" && $"b.target" < $"c.target" && $"c.target" < $"d.target"))
    )
  val varyDataSize = Seq(1, 128, 256, 512, 1024).map { dataSize =>
    val longsWithData = table("edges").select($"source", $"target", lit("*" * dataSize).as(s"data$dataSize"))
    longsWithData.persist(StorageLevel.MEMORY_AND_DISK)
    println(s"vary data edges num: ${longsWithData.count()}")
    new Query(
      s"multi-join - datasize: $dataSize",
      longsWithData.as("a").join(longsWithData.as("b"), $"a.target" === $"b.source")
        .join(longsWithData.as("c"), $"b.target" === $"c.source" && $"c.target" === $"a.source")
        .where($"a.source" < $"b.source" && $"b.source" < $"c.source"))
  }
  val varyNumMatches = {
    val longs = table("edges")
    Seq(1, 2, 4, 8).map { numCopies =>
    val copiedInts = numCopies match {
      case 1 => longs
      case _ =>
        val x = Seq.fill(numCopies)(longs).reduce(_ union _).persist(StorageLevel.MEMORY_AND_DISK)
        println(s"copies of edge num: ${x.count()}")
        x
    }
    new Query(
      s"multi-join - numMatches: $numCopies",
      copiedInts.as("a").join(longs.as("b"), $"a.target" === $"b.source")
        .join(longs.as("c"), $"b.target" === $"c.source" && $"c.target" === $"a.source")
        .where($"a.source" < $"b.source" && $"b.source" < $"c.source"))
  }
  }
}
