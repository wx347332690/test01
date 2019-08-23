package Tag

import Utils.Tags
import org.apache.spark.sql.Row

object TagsChannel extends Tags {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
    val channel: Int = row.getAs[Int]("adplatformproviderid")
//    if (StringUtils.isNotBlank(channel)) {
      list :+= ("CN" + channel, 1)
//    }
    list
  }
}
