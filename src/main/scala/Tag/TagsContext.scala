package Tag

import Utils.TagUtils
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //    if (args.length != 4) {
    //      println("目录不匹配,退出程序")
    //      sys.exit()
    //    }
    //    val Array(inputhPath, outputPath, dirPath, stopPath) = args

    //创建上下文,执行入口 并设置序列化方式,采用kryo的序列化方式,比默认的序列化方式性能高
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    /**
      * 存储到hbase的过程
      */
    //TODO 调用hbase API
    //加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    //创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.host"))
    //创建HbaseConnection
    val hbcon = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbcon.getAdmin
    //判断表是否存在可用
    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
      //创建表操作
      //创建表
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      //创建列簇
      val descriptor = new HColumnDescriptor("tags")
      //将加载到表中并注册该表
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      //关闭hbase连接
      hbadmin.close()
      hbcon.close()
    }


    //TODO 创建JobConf(hadoop任务)
    val jobconf = new JobConf(configuration)
    //指定输出类型
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    //此时指定输出的表,对应的表名
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

    val df: DataFrame = spark.read.parquet("D:\\out_20190820")
    df.show()
    //切分字典集,并生成一个map
    val map = sc.textFile("D:\\Spark项目阶段视频\\项目day01\\Spark用户画像分析\\app_dict.txt").map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collectAsMap()


    //将处理好的数据广播
    val broadcast = sc.broadcast(map)
    //获取停用词库
    val stopword = sc.textFile("D:\\Spark项目阶段视频\\项目day01\\Spark用户画像分析\\stopwords.txt")
      .map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)

    //使用redis存储的字典集实现指标
    //    df.foreachPartition(df => {
    //      val jedis = RedisPooluUtils.getRedis()
    //      df.map(row => {
    //        val appList = TagsAppRedis.makeTags(row, jedis)
    //        appList
    //      }).foreach(println)
    //      jedis.close()
    //    })

    //    过滤符合id的数据
    val baseRDD: RDD[(List[String], Row)] = df.rdd
      .map(row => {
        val userList = TagUtils.getAllUserId(row)
        (userList, row)
      })


    //构建点集合
    val vertexRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2
      //所有标签
      val adList = TagsAd.makeTags(row)
      val appList = TagsApp.makeTags(row, broadcast)
      val keywordList = TagsKeyWords.makeTags(row, bcstopword)
      val dvList = TagsDevice.makeTags(row)
      val locationList = TagsArea.makeTags(row)
      val AllTag = adList ++ appList ++ keywordList ++ dvList ++ locationList
      //list(String,Int)
      //保证其中一个点携带所有标签,同时也能保留所有的userid
      //处理所有的点集合
      val Vd = tp._1.map((_, 0)) ++ AllTag
      tp._1.map(uId => {
        //保证一个点携带标签
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, Vd)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
//    vertexRDD.take(50).foreach(println)


    //构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
    })
//    edges.take(20).foreach(println)

    //构建图
    val graph = Graph(vertexRDD,edges)
    //取出顶点
    val vertices = graph.connectedComponents().vertices
    //处理所有的标签和id
    vertices.join(vertexRDD).map {
      case (uId, (conId, tagsAll)) => (conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })


    //所有的标签都在内部实现
    //      .map(row => {
    //      //取出用户id
    //      val userId = TagUtils.getOneUserId(row)
    //      //接下来通过row数据找到所有标签(按照需求)
    //      val adList = TagsAd.makeTags(row)
    //      val channelList = TagsChannel.makeTags(row)
    //      val areaList = TagsArea.makeTags(row)
    //      val deviceList = TagsDevice.makeTags(row)
    //      val appList = TagsApp.makeTags(row, broadcast)
    //      val keywordList = TagsKeyWords.makeTags(row, bcstopword)
    //      (userId, adList ++ channelList ++ areaList ++ deviceList ++ appList ++ keywordList)
    //    }).rdd.reduceByKey((list1, list2) =>
    //      (list1 ::: list2)
    //        .groupBy(_._1)
    //        .mapValues(_.foldLeft[Int](0)(_ + _._2))
    //        .toList)
    //      //在此处开始往hbase中存储数据,将对应的rowkey存进去
          .map {
          //偏函数(模式匹配的一种)
            case (userid, userTag) => {
              //将对应的rowkey和列对应起来
              val put = new Put(Bytes.toBytes(userid))
              //处理规范标签
              val tags = userTag.map(t => t._1 + "," + t._2).mkString(",")
              //将标签(rowkey)加入到列中
              put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes("20190827"), Bytes.toBytes(tags))
              //返回值
              (new ImmutableBytesWritable(), put)
            }
            //存入到hbase对应表中的hadoop方法
          }.saveAsHadoopDataset(jobconf)


    //    val config = ConfigFactory.load()
    //    val prop = new Properties()
    //    prop.setProperty("user", config.getString("jdbc.user"))
    //    prop.setProperty("password", config.getString("jdbc.password"))
    //    df.write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"), config.getString("jdbc.TableName"), prop)
        spark.stop()
        sc.stop()
  }
}
