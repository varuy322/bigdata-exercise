import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

import scala.collection.immutable.TreeMap
import scala.collection.mutable

/**
 * Created by spark on 5/14/18.
 */
object TopK {

  private val logger=LoggerFactory.getLogger(getClass.getName)

  val inputFileLocation:String=""
  val outputFileLocation:String=""

  def main(args: Array[String]) {

    logger.info("------------------start--------------------")

    val sparkConf=new SparkConf().setAppName("Top-K").setMaster("local")
    val sc=new SparkContext(sparkConf)

    logger.info("----------------------read file------------------")

    val lines=sc.parallelize(List("aa a aa b bb b d b e f g h aa a a a d g "),2)
    doTopK2(lines)
    logger.info("---------done top k-----------")
    sc.stop()
  }

  def doTopK1(lines:RDD[String]):Unit={

    val wordCountRDD=lines.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)
    val sorted=wordCountRDD.map{case(key,value) =>(value,key)}.sortByKey(true,3)
    val topKRDD=sorted.top(4)

    topKRDD.foreach(println)
  }

  def doTopK2(lines:RDD[String]): Unit ={

    //计算每个单词的词频
    val wordCountRDD1= lines.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_)
    //在每一个分区内进行top k查询
    val topK= wordCountRDD1.mapPartitions(iter => {
      val partitionHeap = new Heap()
      while(iter.hasNext){
        partitionHeap.putHeap(iter.next())
      }
      partitionHeap.getHeap().iterator
    })

    val driverHeap = new Heap()
    //将每个分区中统计出来的top k合并成一个新的集合，再统计新集合中的top k。
    topK.collect().foreach(driverHeap.putHeap(_))
    driverHeap.getHeap().foreach(next => logger.info("------"+next._1+"->"+next._2+"-----"))
  }
}

/**
 *
 * @param k
 */
class Heap(k:Int=4){
  /**
   *
   */
  private val hashMap:mutable.Map[String,Int]=new mutable.HashMap[String,Int]()

  implicit val valueOrdering=new Ordering[String]{
    override def compare(x:String,y:String): Int ={
      val xValue:Int=if(hashMap.contains(x)) hashMap.get(x).get else 0
      val yValue:Int=if(hashMap.contains(y)) hashMap.get(y).get else 0

      if(xValue>yValue) -1 else 1
    }
  }

  private var treeMap=new TreeMap[String,Int]()

  /**
   * 把数据存入堆中
   * 自动截取，只保留前面k个数据
   * @param word
   */
  def putHeap(word:(String,Int)): Unit ={
    hashMap+=(word._1->word._2)
    treeMap=treeMap+(word._1->word._2)

    val dropItem=treeMap.drop(k)
    dropItem.foreach(treeMap -=_._1)
    treeMap=treeMap.take(this.k)
  }

  /**
   * 取出堆中的数据
   * @return
   */
  def getHeap():Array[(String,Int)] = {
    val result = new Array[(String, Int)](treeMap.size)
    var i = 0
    this.treeMap.foreach(item => {
      result(i) = (item._1, item._2)
      i += 1
    })
    result
  }

}
