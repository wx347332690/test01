package Tag

import java.util.Properties

import Utils.TagUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("目录不匹配,退出程序")
      sys.exit()
    }
    val Array(inputhPath, outputPath) = args

    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.parquet(inputhPath)
    //过滤符合id的数据
    val ds: Dataset[List[(String, Int)]] = df.filter(TagUtils.OneUserId)
      //所有的标签都在内部实现
      .map(row => {
      //取出用户id
      val userId = TagUtils.getOneUserId(row)
      //接下来通过row数据找到所有标签(按照需求)
      TagsAd.makeTags(row)

    })
    ds.show()

    val config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user", config.getString("jdbc.user"))
    prop.setProperty("password", config.getString("jdbc.password"))
    ds.write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"), config.getString("jdbc.TableName"), prop)

    spark.stop()
  }
}
