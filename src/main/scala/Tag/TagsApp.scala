package Tag

import Utils.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsApp extends Tags {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //处理参数类型
    val row = args(0).asInstanceOf[Row]
    val appmap = args(1).asInstanceOf[Broadcast[Map[String,String]]]
    val appname: String = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if (StringUtils.isNotBlank(appname)) {
      list :+= ("APP" + appname, 1)
    }else if(StringUtils.isNotBlank(appid)){
      list:+=("APP"+appmap.value.getOrElse(appid,appname),1)
    }
    list
  }
}
