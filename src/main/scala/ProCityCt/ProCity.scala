package ProCityCt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * t统计各省市地域分布情况
  */
object ProCity {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[1]")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    val df: DataFrame = spark.read.parquet("D:\\out_2019-08-20")
    df.registerTempTable("log")
    val result = spark.sql("select provincename,cityname,count(*) as count from log group by provincename,cityname")
    //数据倾斜使用coalesce进行减少分区
    //partitionBy进行分区存储,可以设置多个子目录
    //result.coalesce(1).write.partitionBy("provincename","cityname").json("d://out-20198021")


    //加载配置文件需要对应的依赖包
    val config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user", config.getString("jdbc.user"))
    prop.setProperty("password", config.getString("jdbc.password"))
    result.write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"), config.getString("jdbc.TableName"), prop)
    sc.stop()
  }
}
