package main.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by spark on 4/20/18.
 * dataforma referred by:http://cdiac.ess-dive.lbl.gov/ftp/ndp040/data_format.html
 */
object DropRain {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("ussr-dropRain").setMaster("spark://master1:7077")
    val sc=new SparkContext(conf)

    val pre_data=sc.textFile("hdfs://master1:9000//ussr/")
    val fields=pre_data.map(line => line.trim().replace("  "," ")split(" ")).filter(_.length==15).filter(_(13)!="9").filter(_(11)!="999.9")

    val pre_avg=fields.map(field => (field(1),field(11).toDouble)).groupByKey().map{ x=> (x._1,x._2.reduce(_+_)/x._2.count(x=>true))}

    val pre_sort=pre_avg.map(x =>(x._2 * 365,x._1)).sortByKey(false).map(x =>(x._2,x._1))
    pre_sort.saveAsTextFile("hdfs://master1:9000/ussr/chapter4/output1")
    //pre_sort.take(10).foreach(println)
    sc.stop()
  }

}
