package Tag

import Utils.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object TagsArea extends Tags{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]

    val provincename:String = row.getAs[String]("provincename")
    if(StringUtils.isNotBlank(provincename)){
      list:+=("ZP"+provincename,1)
    }
    val cityname:String = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(cityname)){
      list:+=("ZC"+cityname,1)
    }

    list
  }
}
