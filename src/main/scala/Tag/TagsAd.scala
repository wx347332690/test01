package Tag

import Utils.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object TagsAd extends Tags {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
    //获取广告类型,广告类型名称,渠道
    val adtype: Int = row.getAs[Int]("adspacetype")
    adtype match {
      case v if v > 9 => list :+= ("LC" + v, 1)
      case v if v <= 9 && v > 0 => list :+= ("LC0" + v, 1)
    }
    val adname: String = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adname)) {
      list :+= ("LN" + adname, 1)
    }



    list
  }
}
