package guat.lxy

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val dataRDD = sc.parallelize(args(0))
    var words = dataRDD.flatMap(line => line.split(" ")).map(word => word,1).reduceByKey(_+_)
    words.collect().foreach(println)
    sc.stop()
  }
}
