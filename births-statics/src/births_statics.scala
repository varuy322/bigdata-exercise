import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 5/5/18.
 */
object births_statics {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("births-tatics-880-2016").setMaster("local[2]") //"spark://master1:7077"
    val sc = new SparkContext(conf)

    val HDFS: String = "hdfs://master1:9000//names"

    val birth_rdd = sc.wholeTextFiles(HDFS, minPartitions = 40)

    val data_rdd = birth_rdd
      .map(x => {
      val fn = x._1
      val len = fn.length
      val year = fn.substring(len - 8, len - 4)
      x._2.split("\r\n").map(x => (year, x))
    })

    val pro_rdd = data_rdd
      .flatMap(x => x)
      .map(x => (x._1, x._2.split(",")(2).toInt))
      .reduceByKey(_ + _).cache()

    pro_rdd.collect().foreach(println)
    pro_rdd.saveAsTextFile("/home/spark/dataset/names/birth_1880_2016")

    sc.stop()
  }
}
