package com.databricks.spark.sql.perf

import java.io.{File, FilenameFilter}

import com.databricks.spark.sql.perf.ExecutionMode.CountResults
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.io.Source

/**
 * Created by wuxiaoqi on 17-1-4.
 */
class CircleQueryPerformance extends Benchmark {

  import sqlContext.implicits._

  val dir = new File(sqlContext.getConf("spark.sql.perf.dataSource"))
  val edges = dir.listFiles(new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = name.endsWith("edges")
  })
  val circles = dir.listFiles(new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = name.endsWith("circles")
  })

  def df(name: String, files: Array[File], partitions: Int, fields: StructField*): DataFrame = {
    val schema = StructType(fields)
    val generatedData = {
//      sqlContext.sparkContext.parallelize(1L to 2000L, 4).flatMap {
//        case x=>
//          Seq.fill(2)((x, x))
//      }
      sqlContext.sparkContext.parallelize(files.flatMap { f =>
        val ego = f.getName.substring(0, f.getName.indexOf(".")).toLong
        Source.fromFile(f).getLines().flatMap {
          case line =>
            val ps = line.split("\\s+").map(_.toLong)
            if (ps.length == 2) {
              Seq[(Long, Long)]((ego, ps(0)), (ego, ps(1)), (ps(0), ps(1)))
            } else {
              ps.sliding(2).map(x => (x(0), x(1))).toSeq :+ (ego, ps(0)) :+ (ps(ps.length - 1), ego)
            }
        }
      }, partitions).repartition(partitions)
    }

    generatedData.setName(s"$name")

    val rows = generatedData.mapPartitions { iter =>
      iter.map { l =>
        Row.fromTuple(l)
      }
    }
//    sqlContext.createDataFrame(rows, schema).write.json("twitter")
//    println(s"df size: ${rows.count()}")
    sqlContext.createDataFrame(rows, schema)
  }

  val joinTables = Seq(
    Table("edges", df(s"edges", edges, sqlContext.getConf(SQLConf.SHUFFLE_PARTITIONS.key).toInt / 8,
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
  //  private val table = sqlContext.table _
  val varyDataSize = Seq(1, 128, 256, 512, 1024).map { dataSize =>
    val longsWithData = joinTables(0).data.select($"source", $"target", lit("*" * dataSize).as(s"data$dataSize"))
    new Query(
      s"multi-join - datasize: $dataSize",
      longsWithData.as("a").join(longsWithData.as("b"), $"a.target" === $"b.source")
        .join(longsWithData.as("c"), $"b.target" === $"c.source" && $"c.target" === $"a.source"),
      executionMode = CountResults)
  }
  val varyNumMatches = Seq(1, 2, 4, 8, 16).map { numCopies =>
    val longs = joinTables(0).data
    val copiedInts = Seq.fill(numCopies)(longs).reduce(_ union _)
    new Query(
      s"multi-join - numMatches: $numCopies",
      copiedInts.as("a").join(longs.as("b"), $"a.target" === $"b.source")
        .join(longs.as("c"), $"b.target" === $"c.source" && $"c.target" === $"a.source"),
      executionMode = CountResults)
  }
}
