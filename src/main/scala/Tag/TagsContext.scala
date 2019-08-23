package Tag

import Utils.TagUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

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

    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[1]")
    val  sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    val df: DataFrame = spark.read.parquet("D:\\out_20190820")

    val map = sc.textFile("D:\\Spark项目阶段视频\\项目day01\\Spark用户画像分析\\app_dict.txt").map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collectAsMap()

    //将处理好的数据广播
    val broadcast = sc.broadcast(map)
    //获取停用词库
    val stopword = sc.textFile("D:\\Spark项目阶段视频\\项目day01\\Spark用户画像分析\\stopwords.txt").map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)

    //过滤符合id的数据
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

      (userId,adList,channelList,areaList,deviceList,appList,keywordList)
    }).collect.foreach(println)



    //    val config = ConfigFactory.load()
    //    val prop = new Properties()
    //    prop.setProperty("user", config.getString("jdbc.user"))
    //    prop.setProperty("password", config.getString("jdbc.password"))
    //    ds.write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"), config.getString("jdbc.TableName"), prop)

  }
}
