package etl

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.{DataFrame, SparkSession}
object parquet2RDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    //    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext()
    val ssc: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    val df: DataFrame = ssc.read.parquet("D:\\out_2019-08-20")
    val rdd: RDD[(String, String)] = df.rdd
      .map(x => (x.getString(0), x.getString(1)))

    rdd.collect().foreach(println)
sc.stop()
ssc.stop()


  }
}
