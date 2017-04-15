package com.databricks.spark.sql.perf

import java.net.InetAddress

import com.databricks.spark.sql.perf.tpcds.{MultiJoinTPCDS, TPCDS, Tables}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

import scala.util.Try

/**
 * Created by wuxiaoqi on 17-3-16.
 */
object RunTPCDSBenchmark {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunConfig]("spark-sql-perf") {
      head("spark-sql-perf", "0.2.0")
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
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
        .text("The scale of partitions after shuffle")
      opt[Int]('C', "Cardinality")
        .action((x, c) => c.copy(cardinality = Some(x)))
        .text("The cardinality")
      opt[Int]('t', "tries")
        .action((x, c) => c.copy(tries = Some(x)))
        .text("The number of estimations")
      opt[String]('d', "dsdgenDir")
        .action((x, c) => c.copy(dsdgenDir = Some(x)))
        .text("The location of dsdgen tool installed in your machines")
        .required()
      opt[String]('l', "location")
        .action((x, c) => c.copy(location = Some(x)))
        .text("The location for saving TPC-DS tables")
        .required()
      opt[Int]('S', "scale")
        .action((x, c) => c.copy(scale = Some(x)))
        .text("Volume of data to generate in GB. Default 1.")

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
      .set("spark.memory.storageFraction", "0.2")
      .set("spark.memory.fraction", "0.7")
      .set("spark.driver.maxResultSize", "20G")

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
    if (!config.location.isDefined) {
      throw new IllegalArgumentException("location is not set. Please use --help to see usage.")
    }

    // scale is volume of data to generate in GB. Default 1.
    val scale = config.scale match {
      case Some(s) => s
      case _ => 1
    }
    val tables = config.dsdgenDir match {
      case Some(dsdgenDir) => new Tables(sqlContext, dsdgenDir, scale)
      case _ => throw new IllegalArgumentException("dsdgenDir must be set.")
    }
    tables.genData(config.location.get, "json", false, true, false, false, false)
    tables.createTemporaryTables(config.location.get, "json", "store_sales")
    tables.createTemporaryTables(config.location.get, "json", "customer_demographics")
    tables.createTemporaryTables(config.location.get, "json", "item")
    tables.createTemporaryTables(config.location.get, "json", "promotion")
    tables.createTemporaryTables(config.location.get, "json", "date_dim")
    tables.createTemporaryTables(config.location.get, "json", "customer")
    tables.createTemporaryTables(config.location.get, "json", "customer_address")
    tables.createTemporaryTables(config.location.get, "json", "store")
    tables.createTemporaryTables(config.location.get, "json", "inventory")

    val benchmark = new MultiJoinTPCDS(sqlContext = sqlContext)
    println("== QUERY LIST ==")
    benchmark.queries.foreach(println)
    val allQueries = config.filter.map { f =>
      benchmark.queries.filter(_.name contains f)
    } getOrElse {
      benchmark.queries
    }

    val variations = benchmark.strategiesChoosing

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
    println("== Choosing Strategies ==")
    experiment.getCurrentRuns().where($"tags".getItem("strategiesChoosing") === "on")
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
