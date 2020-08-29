package org.formation

import org.apache.spark._
import org.apache.spark.streaming._
import org.slf4j.LoggerFactory

object SparkStreamingTP3 {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(SparkStreamingTP3.getClass)
    logger.info("Starting word count streaming Application")
    val conf = new SparkConf().setAppName("MySparkStreamingApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val wordsCounts = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    wordsCounts.print();
  }
}
