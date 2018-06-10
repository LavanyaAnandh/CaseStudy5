import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level,Logger}

object CaseStudy5 {
  def main(args: Array[String]): Unit = {
    println("Spark Streaming")
    val directory = "/home/acadgild/casestudy5"
    println(s" $directory")
    val conf = new SparkConf().setMaster("local[2]").setAppName("CaseStudy5")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(15))
    val lines = ssc.textFileStream(args(0))
    //lines.print()
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
