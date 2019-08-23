package Tag

import Utils.Tags
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKeyWords extends Tags {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[Map[String,String]]]
    //获取关键字,打标签
    val keys = row.getAs[String]("keywords").split("\\|")

    keys.filter(word => {
      word.length >= 3 && word.length <= 8 && !stopword.value.contains(word)
    }).foreach(word=>list:+=("K"+word,1))
  list
  }
}
