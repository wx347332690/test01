package Utils

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 商圈解析工具
  */
object AliMapUtil {
  //获取高德地图商圈信息
  def getBussinessFromAliMap(long: Double, lat: Double): String = {
    val location = long + ","+ lat
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?&location="+location+"&key=fc141c70afa2cc2b107ae1cacd379c0b&radius=1000&extensions=all"

    //调用请求
    val jsonstr: String = HttpUtil.get(urlStr)
    //解析json串
    val jsonparse: JSONObject = JSON.parseObject(jsonstr)
    //判断状态是否成功
    val status = jsonparse.getIntValue("status")
    if (status == 0) return ""
    //接下来解析内部json串,判断每个key的value都不能为空
    val regeocodesJson = jsonparse.getJSONObject("regeocode")
    if (regeocodesJson == null || regeocodesJson.keySet().isEmpty) return ""
    val addressComponentJson = regeocodesJson.getJSONObject("addressComponent")
    val bussnissAreasArray = addressComponentJson.getJSONArray("businessAreas")
    if (bussnissAreasArray == null || bussnissAreasArray.isEmpty) return null
    //创建集合保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    //循环输出
    for (item <- bussnissAreasArray.toArray) {
      if (item.isInstanceOf[JSONObject]) {
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }

    buffer.mkString(",")
  }
}
