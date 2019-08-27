package Tag

import Utils.{AliMapUtil, RedisPooluUtils, Tags, Utils2Type}
import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsBusiness extends Tags {

  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    // 获取经纬度，过滤经纬度
    if(Utils2Type.toDouble(long)>= 73.0 &&
      Utils2Type.toDouble(long)<= 135.0 &&
      Utils2Type.toDouble(lat)>=3.0 &&
      Utils2Type.toDouble(lat)<= 54.0){
      // 先去数据库获取商圈
      val business = getBusiness(long.toDouble,lat.toDouble)
      // 判断缓存中是否有此商圈
      if(StringUtils.isNotBlank(business)){
        val lines = business.split(",")
        lines.foreach(f=>list:+=(f,1))
      }
      //      list:+=(business,1)
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long: Double, lat: Double): String = {
    //转换geoHash字符串,第三位是代表精确几位的经纬度信息,越大越详细
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)
   //去数据库查询
    var business = redis_queryBusiness(geoHash)
    //判断商圈是否为空
    if (business == null || business.length == 0) {
      business = AliMapUtil.getBussinessFromAliMap(long.toDouble, lat.toDouble)
    //如果调用高德地图商圈,需要将此次商圈存入redis
      redis_insertBusiness(geoHash,business)
    }
    redis_insertBusiness(geoHash,business)
    business
  }

  /**
    * 查询商圈信息
    */
  def redis_queryBusiness(geoHash: String): String = {
    val jedis = RedisPooluUtils.getRedis()
    val business = jedis.get(geoHash)
    jedis.close()
    business
  }

  /**
    * 存储商圈信息到redis
    */
  def redis_insertBusiness(geoHash: String,business: String): Unit = {
    val jedis = RedisPooluUtils.getRedis()
    jedis.set(geoHash,business)
    jedis.close()

  }
}
