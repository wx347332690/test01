package Tag

import Utils.Tags
import org.apache.spark.sql.Row

object TagsKeyWords extends Tags{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
  //keywords: String,
    val keyWords:String = row.getAs[String]("keywords")
    if (keyWords.contains("|")) {
      val keys: Array[String] = keyWords.split("\\|")
      if (keys.length >= 3 && keys.length <= 8) {
        list :+= ("K" + keyWords, 1)
      }
    }else{
      list:+=("K"+keyWords,1)
    }
    list
  }
}
