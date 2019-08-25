package Utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisPooluUtils {
  //初始化jedisPool环境
  private val conf: JedisPoolConfig = new JedisPoolConfig
  //设置JedisPoolConfig环境
  conf.setMaxTotal(30)
  conf.setMaxIdle(10)

  //初始化一个Jedispool
  private val pool: JedisPool = new JedisPool(conf, "Hadoop01", 6379,10000)

  //获取一个Jedis连接
  def getRedis(): Jedis = {
    pool.getResource
  }

}
