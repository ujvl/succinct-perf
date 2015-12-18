package edu.berkeley.cs.succinct.perf

import java.io.FileWriter

import edu.berkeley.cs.succinct.kv._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag
import scala.util.Random

object SysBenchBatch {
  // Constants
  val WARMUP_COUNT: Int = 200
  val MEASURE_COUNT: Int = 2000

  val WARMUP_TIME: Int = 30000
  val MEASURE_TIME: Int = 120000
  val COOLDOWN_TIME: Int = 30000

  // Queries
  var keys: Array[Long] = _
  var keysWarmup: Array[Long] = _
  var keysMeasure: Array[Long] = _

  // Output path
  var outPath: String = _

  // Number of threads
  val numThreads = Seq(1, 2, 4, 8, 16)

  // Batch Sizes
  val batchSizes = Seq(10, 20, 40, 80, 160)

  def sampleArr[T: ClassTag](input: Array[T], sampleSize: Int): Array[T] = {
    Array.fill(sampleSize)(input(Random.nextInt(input.length)))
  }

  def benchSuccinctRDDLatency(rdd: SuccinctKVRDD[Long]): Unit = {
    batchSizes.foreach(b => {
      println(s"Benchmarking Succinct RDD get for batch size = $b...")
      val batchesWarmup = (0 to (WARMUP_COUNT/b) - 1).map(i => keysWarmup.slice(i, i + b))
      val batchesMeasure = (0 to (MEASURE_COUNT/b) - 1).map(i => keysMeasure.slice(i, i + b))
      batchesWarmup.foreach(k => {
        val size = rdd.multiget(k).size
        println(s"$k\t$size")
      })

      // Measure
      val outGet = new FileWriter(outPath + "/get-latency-$b")
      batchesMeasure.foreach(k => {
        val startTime = System.currentTimeMillis()
        val size = rdd.multiget(k).size
        val endTime = System.currentTimeMillis()
        val totTime = endTime - startTime
        outGet.write(s"$k\t$size\t$totTime\n")
      })
      outGet.close()
    })
  }

  def benchSuccinctRDDThroughput(rdd: SuccinctKVRDD[Long]): Unit = {
    // Get queries
    batchSizes.foreach(b => {
      numThreads.foreach(t => {
        val threads = (0 to (t - 1)).map(tid =>
          new Thread(new Runnable {
            override def run(): Unit = {
              // Warmup
              println(s"$tid: Starting Warmup Phase...")
              var startTime = System.currentTimeMillis()
              var i = 0
              while (System.currentTimeMillis() - startTime < WARMUP_TIME) {
                // Prepare batch
                val batch = keys.slice(i, i + b)
                rdd.multiget(batch)
                i = (i + b) % keys.length
              }

              // Measure
              println(s"$tid: Starting Measure Phase...")
              startTime = System.currentTimeMillis()
              i = 0
              var numQueries = 0
              while (System.currentTimeMillis() - startTime < MEASURE_TIME) {
                val batch = keys.slice(i, i + b)
                rdd.multiget(batch)
                i = (i + b) % keys.length
                numQueries += b
              }
              val time = (System.currentTimeMillis() - startTime).toDouble / 1000.0
              val throughput = numQueries.toDouble / time.toDouble
              val out = new FileWriter(outPath + s"/get-throughput-$b-$t", true)
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
    })
  }

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: SysBench <succinct-data> <output-path> <bench-type>")
      System.exit(1)
    }

    val succinctDataPath = args(0)
    outPath = args(1)
    val benchType = args(2)

    val sparkConf = new SparkConf().setAppName("SysBench")
    val ctx = new SparkContext(sparkConf)

    val kvRDDSuccinct = ctx.succinctKV[Long](succinctDataPath)
    val count = kvRDDSuccinct.count()

    keys = Random.shuffle((0 to 9999).map(i => Math.abs(Random.nextLong()) % count)).toArray
    keysWarmup = sampleArr(keys, WARMUP_COUNT)
    keysMeasure = sampleArr(keys, MEASURE_COUNT)

    if (benchType == "throughput")
      benchSuccinctRDDThroughput(kvRDDSuccinct)
    else if (benchType == "latency")
      benchSuccinctRDDLatency(kvRDDSuccinct)
    else
      println(s"[ERROR] Invalid benchType: $benchType")
  }
}
