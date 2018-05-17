import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 5/18/18.
 */
object SecondSort {
  def main(args: Array[String]) {

    val filename = "hdfs://192.168.192.120:9000//input/secondsort.txt"

    val conf = new SparkConf()
      .setAppName("secondsort-spark")

    val sc = new SparkContext(conf)


    val fileRdd = sc.textFile(filename)

    val dataRdd = fileRdd.map(_.split(','))
      .map(x => ((x(0), x(1)), x(3)))
      .groupByKey()
      .sortByKey(false)
      .map(x => (x._1._1 + "-" + x._1._2, x._2.toList.sortWith(_ > _)))

    dataRdd.foreach(
      x => {
        val buf = new StringBuilder()
        for (a <- x._2) {
          buf.append(a)
          buf.append(",")
        }
        buf.deleteCharAt(buf.length - 1)
        println(x._1 + " " + buf.toString())
      }
    )

    sc.stop()

  }
}
