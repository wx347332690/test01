package Rpt

import Utils.RptUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object AppRpt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    val sc = new SparkContext(conf)
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val df: DataFrame = spark.read.parquet("D:/out_20190820")
    val rdd: RDD[(String, String)] = sc.textFile("D:\\Spark项目阶段视频\\项目day01\\Spark用户画像分析\\app_dict.txt")
      .map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))


    val logs: Map[String, String] = rdd.collect().toMap
    logs.foreach(println)
    //将字典数据进行广播
    val broadcastMediaInfo: Broadcast[Map[String, String]] = sc.broadcast(logs)

    import spark.implicits._
    val res: RDD[(String, Double, Double, Double, Double, Double, Double, Double, Double, Double)] = df.map(row => {
      //增加key:appname.并判断其是否为空
      var appname: String = row.getAs[String]("appname")
      if (StringUtils.isBlank(appname)) {
        appname = broadcastMediaInfo.value.getOrElse(row.getAs[String]("appid"), "unknown")
      }
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //调用三个方法
      val request = RptUtils.request(requestmode, processnode)
      val click = RptUtils.click(requestmode, iseffective)
      val ad = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      (appname, request ++ click ++ ad)
    }).rdd.reduceByKey((list1, list2) =>
      (list1 zip list2)
        .map(x => x._1 + x._2)
    ).map(x => {
      (x._1, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8))
    }
    )

    res.collect().foreach(println)


    //    //Rdd转换为df存储在mysql中
//    val dataframe: DataFrame = spark.createDataFrame(res)
//    val config = ConfigFactory.load()
//    val prop = new Properties()
//    prop.setProperty("user", config.getString("jdbc.user"))
//    prop.setProperty("password", config.getString("jdbc.password"))
//    dataframe.write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"), config.getString("jdbc.TableName"), prop)

    spark.stop()
    sc.stop()
  }
}

