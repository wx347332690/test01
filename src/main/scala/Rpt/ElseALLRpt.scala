//package Rpt
//
//import java.util.Properties
//
//import com.typesafe.config.ConfigFactory
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//import org.apache.spark.{SparkConf, SparkContext}
//import Utils.RptUtils
//
//object ElseALLRpt {
//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[1]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    val sc = new SparkContext(conf)
//    val ssc = SparkSession.builder.config(conf).getOrCreate()
//    val df: DataFrame = ssc.read.parquet("D:\\out_20190820")
//    import ssc.implicits._
//    val res: RDD[(String, Double, Double, Double, Double, Double, Double, Double, Double, Double)] = df.map(row => {
//      //把需要的字段全部取出
//      val requestmode: Int = row.getAs[Int]("requestmode")
//      val processnode: Int = row.getAs[Int]("processnode")
//      val iseffective: Int = row.getAs[Int]("iseffective")
//      val isbilling: Int = row.getAs[Int]("isbilling")
//      val isbid: Int = row.getAs[Int]("isbid")
//      val iswin: Int = row.getAs[Int]("iswin")
//      val adorderid = row.getAs[Int]("adorderid")
//      val winprice = row.getAs[Double]("winprice")
//      val adpayment = row.getAs[Double]("adpayment")
//
//      //key:运营商字段
//      //   val ispname: String = row.getAs[String]("ispname")
//      //key:网络类型
//      val networkmannername: String = row.getAs[String]("networkmannername")
//
//      //key:设备类型
//      //      val devicetype: Int = row.getAs[Int]("devicetype")
//      //key:操作系统
//      //          val client:Int = row.getAs[Int]("client")
//
//      //媒体类型:mediatype
//      //      val apptype: Int = row.getAs[Int]("apptype")
//
//      //      val osversion: String = row.getAs[String]("osversion")
//      val request = RptUtils.request(requestmode, processnode)
//      val click = RptUtils.click(requestmode, iseffective)
//      val ad = RptUtils.Ad(iseffective, isbilling, isbid,iswin, adorderid, winprice, adpayment)
//      //   (ispname, request++ad++click)
//      (networkmannername, request ++ ad ++ click)
//      //      (client, request ++ ad ++ click)
//      //      (devicetype, request ++ ad ++ click)
//      //      (osversion, request ++ ad ++ click)
//      //         (apptype, request ++ ad ++ click)
//    }).rdd
//      .reduceByKey((list1, list2) => {
//        list1.zip(list2).map(x => x._1 + x._2)
//      }).map(x => {
//      (x._1, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8))
//    })
//
//
//    //Rdd转换为df存储在mysql中
//    val config = ConfigFactory.load()
//    val prop = new Properties()
//    prop.setProperty("user", config.getString("jdbc.user"))
//    prop.setProperty("password", config.getString("jdbc.password"))
//    res.toDF().write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"), config.getString("jdbc.TableName"), prop)
//
//    //    res.saveAsTextFile("d:/out-20190821-6")
//    sc.stop()
//    ssc.stop()
//  }
//}
