package com.databricks.spark.sql.perf

import java.io.{File, FilenameFilter}

import com.databricks.spark.sql.perf.ExecutionMode.{CollectResults, CountResults}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, _}

import scala.io.Source

/**
 * Created by wuxiaoqi on 17-1-4.
 */
class SquareQueryPerformance extends Benchmark {

  import sqlContext.implicits._

  val edge = sqlContext.getConf("spark.sql.perf.dataSource")
//  val edges = dir.listFiles(new FilenameFilter {
//    override def accept(dir: File, name: String): Boolean = name.contains("edge")
//  })

  def df(name: String, file: String, partitions: Int, fields: StructField*): DataFrame = {
    val schema = StructType(fields)
    val startTime = System.nanoTime()
    val generatedData = {
      sqlContext.sparkContext.textFile(file, partitions).map {
        case line =>
          val ps = line.split("\\s+").map(_.toLong)
          (ps(0), ps(1))
      }
    }

    generatedData.setName(s"$name")

    val rows = generatedData.mapPartitions { iter =>
      iter.map { l =>
        Row.fromTuple(l)
      }
    }.cache()
    println(s"df size: ${rows.count()}")
    println(s"load data from $startTime, " +
      s"timeout: ${(System.nanoTime() - startTime).toDouble / 1000000}")
    sqlContext.createDataFrame(rows, schema)
  }

  val joinTables = Seq(
    Table("edges", df(s"edges", edge, sqlContext
      .getConf(SQLConf.SHUFFLE_PARTITIONS.key, "16").toInt,
      'source.long,
      'target.long)),
    Table("circles", df(s"circles", edge, sqlContext
      .getConf(SQLConf.SHUFFLE_PARTITIONS.key, "16").toInt,
      'source.long,
      'target.long))
  )
  //  joinTables.map {
  //    case t =>
  //      t.get.data
  //        .write
  //        .mode("overwrite")
  //        .saveAsTable(t.name)
  //  }


  private val table = sqlContext.table _
  val triangleAndSquareQueries = Seq(0, 1).map { x =>
    val longs = joinTables(0).data
    x match {
      case 0 =>
        // find squares in graph
        new Query(
          s"multi-join - find squares",
          longs.as("a").join(longs.as("b"), $"a.source" === $"b.source")
            .join(longs.as("c"), $"a.target" === $"c.target")
            .join(longs.as("d"), $"c.source" === $"d.source" && $"d.target" === $"b.target")
            .where($"c.target" =!= $"b.target" && $"b.source" =!= $"c.source"),
          executionMode = CountResults)
      case 1 =>
        // find triangles in graph
        new Query(
          s"multi-join - find triangles",
          longs.as("a").join(longs.as("b"), $"a.source" === $"b.source")
            .where($"a.target" =!= $"b.target"),
            executionMode = CountResults)
    }
  }
}
