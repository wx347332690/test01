package Rpt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import Utils.RptUtils
/**
  * 地域分布情况
  */
object LocationRptCore {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ssc = SparkSession.builder.config(conf).getOrCreate()
    val dataframe: DataFrame = ssc.read.parquet("D:\\out_20190820")
    import ssc.implicits._
    val res1: RDD[((String, String), List[Double])] = dataframe.map(row => {
      //把需要的字段全部取出
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //key值 是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")

      val request = RptUtils.request(requestmode, processnode)
      val click = RptUtils.click(requestmode, iseffective)
      val ad = RptUtils.Ad(iseffective, isbilling, isbid,iswin, adorderid, winprice, adpayment)
      ((pro, city), request ++ ad ++ click )
    }).rdd
      //根据key进行聚合value
      .reduceByKey(
      (list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)
      })

    res1.saveAsTextFile("d://out-201908222")



    sc.stop()
    ssc.stop()
  }
}
