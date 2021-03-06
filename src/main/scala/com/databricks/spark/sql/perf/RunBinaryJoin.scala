/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf

import java.net.InetAddress

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
 * Created by wuxiaoqi on 17-2-23.
 */
object RunBinaryJoin {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunConfig]("spark-sql-perf") {
      head("spark-sql-perf", "0.2.0")
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
        .required()
      opt[String]('f', "filter")
        .action((x, c) => c.copy(filter = Some(x)))
        .text("a filter on the name of the queries to run")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[Long]('c', "compare")
        .action((x, c) => c.copy(baseline = Some(x)))
        .text("the timestamp of the baseline experiment to compare with")
      opt[String]('s', "source")
        .action((x, c) => c.copy(source = Some(x)))
        .text("The data source of performance testing")
      opt[Int]('p', "parallel")
        .action((x, c) => c.copy(parallel = Some(x)))
        .text("The data source of performance testing")
      opt[Int]('C', "Cardinality")
        .action((x, c) => c.copy(cardinality = Some(x)))
        .text("The data source of performance testing")
      opt[Int]('t', "tries")
        .action((x, c) => c.copy(tries = Some(x)))
        .text("The data source of performance testing")

      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunConfig): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getName)

    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    sqlContext.setConf("spark.driver.allowMultipleContexts", "true")
    sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
    //    sqlContext.setConf("spark.sql.hypercube.strategiesChoosing", strategiesChoosing)
    sqlContext.setConf("spark.sql.hypercube.sampleCardinality",
      config.cardinality.getOrElse(1000).toString)
    sqlContext.setConf("spark.sql.hypercube.sketchTries",
      config.tries.getOrElse(500).toString)
    if (config.parallel.isDefined) {
      sqlContext.setConf(SQLConf.SHUFFLE_PARTITIONS.key, config.parallel.get.toString)
    }
    sqlContext.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
    sqlContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")

    sqlContext.setConf("spark.sql.perf.results", new java.io.File("performance").toURI.toString)
    sqlContext.setConf("spark.sql.perf.dataSource",
      config.source.getOrElse("/home/wuxiaoqi/multi-join/test-data/small-data/"))

    val benchmark = Try {
      Class.forName(config.benchmarkName)
        .newInstance()
        .asInstanceOf[Benchmark]
    } getOrElse {
      Class.forName("com.databricks.spark.sql.perf." + config.benchmarkName)
        .newInstance()
        .asInstanceOf[Benchmark]
    }

    println("== QUERY LIST ==")
    benchmark.allQueries.foreach(println)
    val allQueries = config.filter.map { f =>
      benchmark.allQueries.filter(_.name contains f)
    } getOrElse {
      benchmark.allQueries
    }

    val variations = benchmark.defaultConfig

    println("== QUERY LIST ==")
    allQueries.foreach(println)

    val experiment = benchmark.runExperiment(
      executionsToRun = allQueries,
      iterations = config.iterations,
      variations = Seq(variations),
      tags = Map(
        "runtype" -> "local",
        "host" -> InetAddress.getLocalHost().getHostName()))

    println("== STARTING EXPERIMENT ==")
    experiment.waitForFinish(1000 * 60 * 60 * 3)

    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    println("== Not Choosing Strategies ==")
    experiment.getCurrentRuns().where($"tags".getItem("strategiesChoosing") === "off")
      .withColumn("result", explode($"results"))
      .select("result.*")
      .groupBy("name")
      .agg(
        min($"executionTime") as 'minTimeMs,
        max($"executionTime") as 'maxTimeMs,
        avg($"executionTime") as 'avgTimeMs,
        stddev($"executionTime") as 'stdDev)
      .orderBy("name")
      .show(truncate = false)
    println(s"""Results: sqlContext.read.json("${experiment.resultPath}")""")

    config.baseline.foreach { baseTimestamp =>
      val baselineTime = when($"timestamp" === baseTimestamp, $"executionTime").otherwise(null)
      val thisRunTime = when($"timestamp" === experiment.timestamp, $"executionTime").otherwise(null)

      val data = sqlContext.read.json(benchmark.resultsLocation)
        .coalesce(1)
        .where(s"timestamp IN ($baseTimestamp, ${experiment.timestamp})")
        .withColumn("result", explode($"results"))
        .select("timestamp", "result.*")
        .groupBy("name")
        .agg(
          avg(baselineTime) as 'baselineTimeMs,
          avg(thisRunTime) as 'thisRunTimeMs,
          stddev(baselineTime) as 'stddev)
        .withColumn(
          "percentChange", ($"baselineTimeMs" - $"thisRunTimeMs") / $"baselineTimeMs" * 100)
        .filter('thisRunTimeMs.isNotNull)

      data.show(truncate = false)
    }
  }
}
