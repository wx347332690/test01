package Rpt

import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.RptUtils

object MediaRpt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ssc = SparkSession.builder.config(conf).getOrCreate()



    val maped: RDD[Array[String]] = sc.textFile("E:\\QQ文档\\app_dict.txt")
      .map(_.split("\t", -1))
    val filterd = maped.filter(_.length >= 5)
    val arr: RDD[(String, String)] = filterd.map(arr => {
      (arr(4), arr(1))
    })
    val logs: Map[String, String] = arr.collect().toMap

    //将字典数据进行广播
    val broadcastMediaInfo: Broadcast[Map[String, String]] = sc.broadcast(logs)
    val df: DataFrame = ssc.read.parquet("D:\\out_20190820")
    import ssc.implicits._

    val result: RDD[(String, Double, Double, Double, Double, Double, Double, Double, Double, Double)] = df.map(row => {
        //增加key:appname.并判断其是否为空
        var appname: String = row.getAs[String]("appname")
        if (StringUtils.isBlank(appname)) {
          appname = broadcastMediaInfo.value.getOrElse(row.getAs[String]("appid"), "blank")
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
        val ad = RptUtils.Ad(iseffective, isbilling, isbid, adorderid, winprice, adpayment)
        (appname, request ++ click ++ ad)
      }).rdd
        .reduceByKey((list1, list2) =>
          (list1 zip list2)
            .map(x => x._1 + x._2)
        ).map(x => {
        (x._1, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8))
      }
      )
    result.toDF().collect().foreach(println)

    sc.stop()
    ssc.stop()
  }
}


/*

  //写入数据到mysql的函数
  val data2Mysql = (it: Iterator[(String, Int)]) => {
    var conn: Connection = null;
    var ps: PreparedStatement = null;
    val sql = "insert into location_info(location,counts,access_date) values(?,?,?)"
    val jdbcUrl = "jdbc:mysql://localhost:3306/my_exercise?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val password = "root"

    try {
      conn = DriverManager.getConnection(jdbcUrl, user, password)
      it.foreach(tup => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, tup._1)
        ps.setInt(2, tup._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println(e.printStackTrace())
    }finally {
      if(ps!=null)
        ps.close()
      if (conn!=null)
        conn.close()
    }
  }

}



    df.rdd.map(row=>{
      // 获取媒体类别
      var appname = row.getAs[String]("appname")
      // 如果说我们取到空值的话，那么将取字典文件中进行查询
      if(StringUtils.isBlank(appname)){
        // 通过APPId获取字典文件中对应得APPid
        // 然后取到它的Value
        // com.123.cn   爱奇艺
        appname = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      //val appname = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")

    }).reduceByKey((list1,list2) => {
      // list(0,2,1,5) list2(2,5,4,7)  zip((0,2),(2,5),(1,4),(5,7))
      (list1 zip list2).map(t => t._1 + t._2)
    }).map(t=>{
      t._1 + "," + t._2.mkString(",")
    }).take(10).toBuffer.foreach(println)

    sc.stop()
  }
}

 */