package chapter2

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by spark on 4/15/18.
 */
object WordCount {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("WordCount").setMaster("local")
    val sc=new SparkContext(conf)

    val rdd=sc.textFile(args(0))
    val wc=rdd.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_).map(x =>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    wc.saveAsTextFile(args(1))
    sc.stop()
  }

}
