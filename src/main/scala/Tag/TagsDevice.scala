package Tag

import Utils.Tags
import org.apache.spark.sql.Row

object TagsDevice extends Tags {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    //获取设备信息
    //    client match {
    //      case v if v==1 => list:+=("D0001000"+ v,1)
    //      case v if v==2 => list:+=("D0001000"+ v,1)
    //      case v if v==3 => list:+=("D0001000"+ v,1)
    //      case _  => list:+=("D00010004",1)
    //    }
    val client: Int = row.getAs[Int]("client")
    list :+= ("D0001000" + client, 1)
    val networkmannerid: Int = row.getAs[Int]("networkmannerid")
    list :+= ("D0002000" + networkmannerid, 1)
    val ispid: Int = row.getAs[Int]("ispid")
    list :+= ("D0003000"+ispid,1)


    list
  }
}
