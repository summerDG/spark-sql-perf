package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.ExecutionMode.CountResults
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Created by wuxiaoqi on 17-3-11.
 */
class TrafficEstimation extends Benchmark {
  import sqlContext.implicits._

  def df(name: String, size: Long, partitions: Int, fields: StructField*): DataFrame = {
    val schema = StructType(fields)
    val generatedData = {
      sqlContext.sparkContext.range(0, size)
        .map(x => (x,x)).repartition(partitions)
    }
    generatedData.setName(s"$name")

    val rows = generatedData.mapPartitions { iter =>
      iter.map { l =>
        Row.fromTuple(l)
      }
    }
    sqlContext.createDataFrame(rows, schema)
  }


//  val tables = Seq (Table("1mil", df("1mil", 1000000L, 1, 'source.long, 'target.long)),
//    Table("5mil", df("5mil", 5000000L, 5, 'source.long, 'target.long)),
//    Table("10mil", df("10mil", 10000000L, 10, 'source.long, 'target.long)),
//    Table("50mil", df("50mil", 50000000L, 10, 'source.long, 'target.long)),
//    Table("100mil", df("100mil", 100000000L, 10, 'source.long, 'target.long)))
  val tables = Seq[Double](0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
  .map {
    case skew =>
      val df = sqlContext.read.json(sqlContext.getConf("spark.sql.perf.dataSource") + s"/skew-$skew")
      Table(s"skew-$skew", df)
  }
//  val numMatches = Seq(1, 2, 4, 8, 16).map { numCopies =>
//    val longs = tables(0).data
//    val copiedInts = Seq.fill(numCopies)(longs).reduce(_ union _)
//    new Query(
//      s"multi-join - numMatches: $numCopies",
//      copiedInts.as("a").join(longs.as("b"), $"a.target" === $"b.source")
//        .join(longs.as("c"), $"b.target" === $"c.source" && $"c.target" === $"a.source"))
//  }
  val selfCircles = tables.map { table =>
    val longs = table.data
    new Query(
      s"self-join-circle: ${table.name}",
      longs.as("a").join(longs.as("b"), $"a.target" === $"b.source")
        .join(longs.as("c"), $"b.target" === $"c.source" && $"c.target" === $"a.source")
    )
  }
//  val selfLines = tables.map { table =>
//    val longs = table.data
//    new Query(
//      s"self-join-line: ${table.name}",
//      longs.as("a").join(longs.as("b"), $"a.target" === $"b.source")
//        .join(longs.as("c"), $"b.target" === $"c.source")
//    )
//  }
//  val diffCircles = Seq ((0, 1, 2), (1, 2, 3), (2,3,4), (3,4,0)).map {
//    case (l, m, r) =>
//      val left = tables(l).data
//      val mid = tables(m).data
//      val right = tables(r).data
//      new Query(
//        s"diff-table-circle: ${tables(l).name}, ${tables(m).name}, ${tables(r).name}",
//        left.as("a").join(mid.as("b"), $"a.target" === $"b.source")
//          .join(right.as("c"), $"b.target" === $"c.source" && $"c.target" === $"a.source")
//      )
//  }
//  val diffLines = Seq ((0, 1, 2), (1, 2, 3), (2,3,4), (3,4,0)).map {
//    case (l, m, r) =>
//      val left = tables(l).data
//      val mid = tables(m).data
//      val right = tables(r).data
//      new Query(
//        s"diff-table-line: ${tables(l).name}, ${tables(m).name}, ${tables(r).name}",
//        left.as("a").join(mid.as("b"), $"a.target" === $"b.source")
//          .join(right.as("c"), $"b.target" === $"c.source")
//      )
//  }
}
