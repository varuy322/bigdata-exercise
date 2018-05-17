package chapter5

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 5/15/18.
 */
object KMeansKdd {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("kddCup").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rawData = sc.textFile("hdfs://master1:9000/kddcup.data.corrected").cache()

    rawData.map(_.split(",").last).countByValue().toSeq.reverse.foreach(println)

    //删除下标从1开始的三个类别型列和最后的标号列 not numeric type
    val labelsAndData = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)

      (label, vector)
    }
    val data=labelsAndData.values.cache()

    val kmeans=new KMeans()
    val model=kmeans.run(data)

    model.clusterCenters.foreach(println)
  }

}
