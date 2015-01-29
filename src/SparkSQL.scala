
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
 * Created by yalin on 2015-01-28.
 */
object Learn_spark {
  def main2(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "d:\\hadoop-2.5.2\\")
    System.setProperty("HADOOP_USER_HOME", "root")
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkSQL")
    val sc = new SparkContext(conf)
    val dataset = sc.textFile("hdfs://Naruto.ccntgrid.zju.edu/user/test/sample_recommend").map(_.split(',')).map(x=>(x(0),x(1))).collect()
    dataset.foreach(println)
  }

  def main3(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "d:\\hadoop-2.5.2\\")
    System.setProperty("HADOOP_USER_HOME", "root")
    System.setProperty("hive.metastore.uris","thrift://Naruto.ccntgrid.zju.edu:9083" )
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("SparkSQL")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("use recommendation")
    //sqlContext.sql("show tables").collect().foreach { println }
    val data = sqlContext.sql("select orderid,productid from recommendation.orderdata limit 100").map(s=>(s(0),s(1)))
    //sqlContext.sql("select * from orderdata limit 20").collect().foreach(println)
    val pairs = data.reduceByKey(_+" "+_)
    val lines = pairs.map(s=>s._1+"---"+s._2).collect()
    lines.foreach(println)
  }

  def main1(args: Array[String]) {
    println("start")
    System.setProperty("hadoop.home.dir", "d:\\hadoop-2.5.2\\")
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkSQL")
    val sc = new SparkContext(conf)
    val data = Array(1,2,2,3,5,3)
    val lines = sc.parallelize(data)
    val pairs = lines.map(s => (s,"a"))
    val result = pairs.reduceByKey(_+";"+_)
    val tmp = result.map(s=>s._1+"---"+s._2)
    tmp.foreach(println)
    println("end")
  }
}
