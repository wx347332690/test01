package Tag

import java.util.Properties

import Utils.{RedisPooluUtils, TagUtils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //    if (args.length != 4) {
    //      println("目录不匹配,退出程序")
    //      sys.exit()
    //    }
    //    val Array(inputhPath, outputPath, dirPath, stopPath) = args

    //创建上下文,执行入口 并设置序列化方式,采用kryo的序列化方式,比默认的序列化方式性能高
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.parquet("D:\\out_20190820")
    df.show()
    //切分字典集,并生成一个map
    val map = sc.textFile("D:\\Spark项目阶段视频\\项目day01\\Spark用户画像分析\\app_dict.txt").map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collectAsMap()

    //将处理好的数据广播
    val broadcast = sc.broadcast(map)
    //获取停用词库
    val stopword = sc.textFile("D:\\Spark项目阶段视频\\项目day01\\Spark用户画像分析\\stopwords.txt")
      .map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)

    //使用redis存储的字典集实现指标
    df.foreachPartition(df => {
      val jedis = RedisPooluUtils.getRedis()
      df.map(row => {
        val appList = TagsAppRedis.makeTags(row, jedis)
        appList
      }).foreach(println)
      jedis.close()
    })
import spark.implicits._
    //    过滤符合id的数据
 df.filter(TagUtils.OneUserId)
      //所有的标签都在内部实现
      .map(row => {
      //取出用户id
      val userId = TagUtils.getOneUserId(row)
      //接下来通过row数据找到所有标签(按照需求)
      val adList = TagsAd.makeTags(row)
      val channelList = TagsChannel.makeTags(row)
      val areaList = TagsArea.makeTags(row)
      val deviceList = TagsDevice.makeTags(row)
      val appList = TagsApp.makeTags(row, broadcast)
      val keywordList = TagsKeyWords.makeTags(row, bcstopword)
      (userId, adList ++ channelList ++ areaList ++ deviceList ++ appList ++ keywordList)
    }).rdd.reduceByKey((list1, list2) =>
      (list1 ::: list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_ + _._2))
        .toList).foreach(println)



    val config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user", config.getString("jdbc.user"))
    prop.setProperty("password", config.getString("jdbc.password"))
//    ds.write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"), config.getString("jdbc.TableName"), prop)
    spark.stop()
    sc.stop()
  }
}
