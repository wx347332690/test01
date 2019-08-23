package Rpt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object LocationRptSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val spark  = new SQLContext(sc)
    val df: DataFrame = spark.read.parquet("D:\\out_20190820")
    df.registerTempTable("logs")
    val res: DataFrame = spark.sql("select provincename,cityname," +
      "sum(case when requestmode==1 and processnode>=1 then 1 else 0 end) sumrequest1," +
      "sum(case when requestmode==1 and processnode>=2 then 1 else 0 end) sumrequest2," +
      "sum(case when requestmode==1 and processnode>=3 then 1 else 0 end) sumrequest3," +
      "sum(case when requestmode==2 and iseffective==1 then 1 else 0 end) sumclick1," +
      "sum(case when requestmode==3 and iseffective==1 then 1 else 0 end) sumclick2," +
      "sum(case when iseffective==1 and isbilling==1 and isbid==1 then 1 else 0 end) sumad1," +
      "sum(case when iseffective==1 and isbilling==1 and winprice==1 and adorderid!=0 then winprice/1000.0 else 0 end) sumad2," +
      "sum(case when iseffective==1 and isbilling==1 and winprice==1 and adorderid!=0 then adpayment/1000.0 else 0 end) sumad3 " +
      "from logs group by provincename,cityname"
    )
        val config = ConfigFactory.load()
        val prop = new Properties()
        prop.setProperty("user", config.getString("jdbc.user"))
        prop.setProperty("password", config.getString("jdbc.password"))
    res.write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"), config.getString("jdbc.TableName"), prop)


    sc.stop()

  }
}
