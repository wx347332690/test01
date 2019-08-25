package Exam01

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JsonRpt {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val json: RDD[String] = sc.textFile("E:\\QQ文档\\json.txt")
    getBusinessTag(json)
    getTypeTag(json)

    def getBusinessTag(json: RDD[String]): Unit = {
      val res: RDD[String] = json.map(line => {
        val jsonobj: JSONObject = JSON.parseObject(line)
        val status: Int = jsonobj.getIntValue("status")
        val buffer = collection.mutable.ListBuffer[String]()
        if (status == 1) {
          val regeoobj: JSONObject = jsonobj.getJSONObject("regeocode")
          if (regeoobj != null && !regeoobj.keySet().isEmpty) {
            val poisarr: JSONArray = regeoobj.getJSONArray("pois")
            if (poisarr != null && !poisarr.isEmpty) {
              for (i <- poisarr.toArray) {
                if (i.isInstanceOf[JSONObject]) {
                  val iobj: JSONObject = i.asInstanceOf[JSONObject]
                  buffer.append(iobj.getString("businessarea"))
                }
              }
            }
          }
        }
        buffer.mkString(";")
      })
      res.flatMap(_.split(";"))
        .map((_, 1))
        .reduceByKey(_ + _)
        .foreach(println)
    }

    def getTypeTag(json: RDD[String]): Unit = {
      val rdd: RDD[String] = json.map(line => {
        val buffer = collection.mutable.ListBuffer[String]()
        val jsonobj: JSONObject = JSON.parseObject(line)
        val status: Int = jsonobj.getIntValue("status")
        if (status == 1) {
          val regeoobj: JSONObject = jsonobj.getJSONObject("regeocode")
          if (regeoobj != null && !regeoobj.keySet().isEmpty) {
            val poisarr: JSONArray = regeoobj.getJSONArray("pois")
            if (poisarr != null && !poisarr.isEmpty) {
              for (i <- poisarr.toArray()) {
                if (i.isInstanceOf[JSONObject]) {
                  val value: JSONObject = i.asInstanceOf[JSONObject]
                  buffer.append(value.getString("type"))
                }
              }
            }
          }
        }
        buffer.mkString(";")
      })
      rdd
        .flatMap(_.split(";"))
        .map((_,1))
        .reduceByKey(_+_)
        .foreach(println)
    }


  }
}


