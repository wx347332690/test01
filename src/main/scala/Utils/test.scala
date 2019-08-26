package Utils

import Tag.TagsBusiness
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试类
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
      * 测试获取指定商圈
      */
    //    val list = List("116.310003,39.991957")
//    val rdd = sc.makeRDD(list)
//    val bs = rdd.map(t => {
//      val arr = t.split(",")
//      AliMapUtil.getBussinessFromAliMap(arr(0).toDouble, arr(1).toDouble)
//    })
//  bs.foreach(println)
    /**
      * 测试通过商圈标签获取商圈
      */
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.parquet("D:\\out_20190820")
    import spark.implicits._
    df.map(row=>{
      val business = TagsBusiness.makeTags(row)
      business
    }).rdd.foreach(println)
  }
}
