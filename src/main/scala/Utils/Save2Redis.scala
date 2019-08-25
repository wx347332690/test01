package Utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Save2Redis {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    val map = sc.textFile("D:\\Spark项目阶段视频\\项目day01\\Spark用户画像分析\\app_dict.txt")
    //读取字段文件
    map.map(_.split("\t", -1))
      .filter(_.length >= 5).foreachPartition(arr=>{
      val jedis = RedisPooluUtils.getRedis()
      arr.foreach(arr=>{
        jedis.set(arr(4),arr(1))
      })
      jedis.close()
    })


sc.stop()
  }
}
