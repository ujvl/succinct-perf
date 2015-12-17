package edu.berkeley.cs.succinct.perf

import java.io.FileWriter

import edu.berkeley.cs.succinct.kv._
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Random

object SysBench {
  // Constants
  val WARMUP_COUNT: Int = 20
  val MEASURE_COUNT: Int = 100

  val WARMUP_TIME: Int = 30000
  val MEASURE_TIME: Int = 120000
  val COOLDOWN_TIME: Int = 30000

  // Queries
  var words: Array[String] = _
  var wordsWarmup: Array[String] = _
  var wordsMeasure: Array[String] = _
  var keys: Array[Long] = _
  var keysWarmup: Array[Long] = _
  var keysMeasure: Array[Long] = _

  // Output path
  var outPath: String = _

  // Number of threads
  val numThreads = Seq(1, 2, 4, 8, 16)

  def sampleArr[T: ClassTag](input: Array[T], sampleSize: Int): Array[T] = {
    Array.fill(sampleSize)(input(Random.nextInt(input.length)))
  }

  def benchSuccinctRDDLatency(rdd: SuccinctKVRDD[Long]): Unit = {
    println("Benchmarking Succinct RDD get...")
    keysWarmup.foreach(k => {
      val length = rdd.get(k).length
      println(s"$k\t$length")
    })

    // Measure
    val outGet = new FileWriter(outPath + "/get-latency")
    keysMeasure.foreach(k => {
      val startTime = System.currentTimeMillis()
      val length = rdd.get(k).length
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outGet.write(s"$k\t$length\t$totTime\n")
    })
    outGet.close()

    println("Benchmarking Succinct RDD search...")

    // Warmup
    wordsWarmup.foreach(w => {
      val count = rdd.search(w).count()
      println(s"$w\t$count")
    })

    // Measure
    val outSearch = new FileWriter(outPath + "/search-latency")
    wordsMeasure.foreach(w => {
      val startTime = System.currentTimeMillis()
      val count = rdd.search(w).count()
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outSearch.write(s"$w\t$count\t$totTime\n")
    })
    outSearch.close()
  }

  def benchSuccinctRDDThroughput(rdd: SuccinctKVRDD[Long]): Unit = {
    // Get queries
    numThreads.foreach(t => {
      val threads = (0 to (t - 1)).map(tid =>
        new Thread(new Runnable {
          override def run(): Unit = {
            // Warmup
            println(s"$tid: Starting Warmup Phase...")
            var startTime = System.currentTimeMillis()
            var i = 0
            while (System.currentTimeMillis() - startTime < WARMUP_TIME) {
              rdd.get(keys(i))
              i = (i + 1) % keys.length
            }

            // Measure
            println(s"$tid: Starting Measure Phase...")
            startTime = System.currentTimeMillis()
            i = 0
            var numQueries = 0
            while (System.currentTimeMillis() - startTime < MEASURE_TIME) {
              rdd.get(keys(i))
              i = (i + 1) % keys.length
              numQueries += 1
            }
            val time = (System.currentTimeMillis() - startTime).toDouble / 1000.0
            val throughput = numQueries.toDouble / time.toDouble
            val out = new FileWriter(outPath + s"/get-throughput-$t", true)
            out.write(s"$tid\t$throughput\n")
            out.close()

            // Cooldown
            println(s"$tid: Starting cooldown phase...")
            startTime = System.currentTimeMillis()
            i = 0
            while (System.currentTimeMillis() - startTime < COOLDOWN_TIME) {
              rdd.get(keys(i))
              i = (i + 1) % keys.length
            }
          }
        })
      )
      threads.foreach(_.start())
      threads.foreach(_.join())
    })

    // Search queries
    numThreads.foreach(t => {
      val threads = (0 to (t - 1)).map(tid =>
        new Thread(new Runnable {
          override def run(): Unit = {
            // Warmup
            println(s"$tid: Starting Warmup Phase...")
            var startTime = System.currentTimeMillis()
            var i = 0
            while (System.currentTimeMillis() - startTime < WARMUP_TIME) {
              rdd.search(words(i)).count()
              i = (i + 1) % words.length
            }

            // Measure
            println(s"$tid: Starting Measure Phase...")
            startTime = System.currentTimeMillis()
            i = 0
            var numQueries = 0
            while (System.currentTimeMillis() - startTime < MEASURE_TIME) {
              rdd.search(words(i)).count()
              i = (i + 1) % words.length
              numQueries += 1
            }
            val time = (System.currentTimeMillis() - startTime).toDouble / 1000.0
            val throughput = numQueries.toDouble / time.toDouble
            val out = new FileWriter(outPath + s"/search-throughput-$t", true)
            out.write(s"$tid\t$throughput\n")
            out.close()

            // Cooldown
            println(s"$tid: Starting cooldown phase...")
            startTime = System.currentTimeMillis()
            i = 0
            while (System.currentTimeMillis() - startTime < COOLDOWN_TIME) {
              rdd.search(words(i)).count()
              i = (i + 1) % words.length
            }
          }
        })
      )
      threads.foreach(_.start())
      threads.foreach(_.join())
    })
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: SysBench <succinct-data> <queries-path> <output-path>")
      System.exit(1)
    }

    val succinctDataPath = args(0)
    val queryPath = args(1)
    outPath = args(2)

    val sparkConf = new SparkConf().setAppName("SysBench")
    val ctx = new SparkContext(sparkConf)

    val kvRDDSuccinct = ctx.succinctKV[Long](succinctDataPath)
    val count = kvRDDSuccinct.count()

    words = Source.fromFile(queryPath).getLines().toArray
    keys = Random.shuffle((0 to 9999).map(i => Math.abs(Random.nextLong()) % count)).toArray

    // Create queries
    keysWarmup = sampleArr(keys, WARMUP_COUNT)
    keysMeasure = sampleArr(keys, MEASURE_COUNT)
    wordsWarmup = sampleArr(words, WARMUP_COUNT)
    wordsMeasure = sampleArr(words, MEASURE_COUNT)

    benchSuccinctRDDLatency(kvRDDSuccinct)
    benchSuccinctRDDThroughput(kvRDDSuccinct)
  }
}
