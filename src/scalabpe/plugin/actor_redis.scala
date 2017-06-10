package scalabpe.plugin

import java.util.Timer
import java.util.TimerTask
import java.util.TreeMap
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.xml.Node

import redis.clients.jedis.BinaryJedis
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.util.Hashing
import scalabpe.core.ArrayBufferInt
import scalabpe.core.Dumpable
import scalabpe.core.Logging
import scalabpe.core.NamedThreadFactory
import scalabpe.core.Router
import scalabpe.core.SelfCheckLike
import scalabpe.core.SelfCheckResult

class RedisActor(override val router: Router, override val cfgNode: Node)
        extends BaseCacheActor(router, cfgNode) with SelfCheckLike with Dumpable {

    init

    override def init() {

        readThreadNumNode = "RedisReadThreadNum"
        writeThreadNumNode = "RedisWriteThreadNum"

        super.init()
        cache = new RedisClient(cacheType, addrs, serviceIds, readThreadNum, writeThreadNum, router, cfgNode)
        log.info("RedisActor started {}", serviceIds)
    }

    override def close() {
        super.close()
        cache.asInstanceOf[RedisClient].close()
        log.info("RedisActor stopped {}", serviceIds)
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {
        val buff = cache.asInstanceOf[RedisClient].selfcheck()
        buff
    }

}

class JedisPoolWrapper(val tag: String, val pool: JedisPool, val backthread: ThreadPoolExecutor, val maxErrorCount: Int) extends Logging {

    val errorCount = new AtomicInteger(0)
    val SELFTESTKEY = "selftest_redis"

    def hasError(): Boolean = {
        try {
            set(SELFTESTKEY, 180, "OK") // 3 minutes
            return false
        } catch {
            case e: Throwable =>
                return true
        }
    }

    def get(key: String): String = {

        var jedis: Jedis = null
        try {

            jedis = getJedis()
            val value = jedis.get(key)
            freeJedis(jedis)
            value

        } catch {
            case e: Throwable =>
                if (jedis != null) {
                    freeBorkenJedis(jedis)
                }
                throw e
        }
    }

    def set(key: String, exp: Int, value: String): Boolean = {

        var jedis: Jedis = null
        try {

            jedis = getJedis()
            val s = if (exp > 0) jedis.setex(key, exp, value) else jedis.set(key, value)
            freeJedis(jedis)
            val cnt = errorCount.intValue()
            if (cnt >= 1) errorCount.decrementAndGet()
            return (s == "OK")

        } catch {
            case e: Throwable =>
                if (jedis != null) {
                    freeBorkenJedis(jedis)
                }
                val cnt = errorCount.incrementAndGet()
                if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                log.error(tag + " set exception key=%s,value=%s".format(key, value))
                throw e
        }

    }

    def setWithNoReply(key: String, exp: Int, value: String) {
        try {
            backthread.execute(new Runnable() {
                def run() {
                    try {
                        set(key, exp, value)
                    } catch {
                        case e: Exception =>
                            log.error(tag + " setWithNoReply exception key=%s,value=%s".format(key, value))
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("redis back queue is full in setWithNoReply")
        }

    }

    def delete(key: String): Boolean = {

        var jedis: Jedis = null
        try {

            jedis = getJedis()
            val k = jedis.del(key)
            freeJedis(jedis)
            val cnt = errorCount.intValue()
            if (cnt >= 1) errorCount.decrementAndGet()
            return (k > 0)
        } catch {
            case e: Throwable =>
                if (jedis != null) {
                    freeBorkenJedis(jedis)
                }
                val cnt = errorCount.incrementAndGet()
                if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                log.error(tag + " delete exception key=%s".format(key))
                throw e
        }

    }

    def deleteWithNoReply(key: String) {
        try {
            backthread.execute(new Runnable() {
                def run() {
                    try {
                        delete(key)
                    } catch {
                        case e: Exception =>
                            log.error("deleteWithNoReply exception key=%s".format(key))
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("redis back queue is full in deleteWithNoReply")
        }
    }

    def incrBy(key: String, exp: Int, value: Long): Long = {

        var jedis: Jedis = null
        try {

            jedis = getJedis()
            val newvalue = jedis.incrBy(key, value)
            if (newvalue == value && exp > 0) {
                jedis.expire(key, exp) // set expire only first time
            }
            freeJedis(jedis)
            val cnt = errorCount.intValue()
            if (cnt >= 1) errorCount.decrementAndGet()
            return newvalue

        } catch {
            case e: Throwable =>
                if (jedis != null) {
                    freeBorkenJedis(jedis)
                }
                val cnt = errorCount.incrementAndGet()
                if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                log.error(tag + " incrBy exception key=%s,value=%s".format(key, value))
                throw e
        }

    }

    def incrByWithNoReply(key: String, exp: Int, value: Long) {
        try {
            backthread.execute(new Runnable() {
                def run() {
                    try {
                        incrBy(key, exp, value)
                    } catch {
                        case e: Exception =>
                            log.error("incrByWithNoReply exception key=%s,value=%s".format(key, value))
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("redis back queue is full in incrByWithNoReply")
        }

    }

    def decrBy(key: String, exp: Int, value: Long): Long = {

        var jedis: Jedis = null
        try {

            jedis = getJedis()
            val newvalue = jedis.decrBy(key, value)
            //if( newvalue == ( value * -1) && exp > 0 ) {
            //jedis.expire(key,exp)
            //}
            freeJedis(jedis)
            val cnt = errorCount.intValue()
            if (cnt >= 1) errorCount.decrementAndGet()
            return newvalue

        } catch {
            case e: Throwable =>
                if (jedis != null) {
                    freeBorkenJedis(jedis)
                }
                val cnt = errorCount.incrementAndGet()
                if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                log.error(tag + " decrBy exception key=%s,value=%s".format(key, value))
                throw e
        }

    }

    def decrByWithNoReply(key: String, exp: Int, value: Long) {
        try {
            backthread.execute(new Runnable() {
                def run() {
                    try {
                        decrBy(key, exp, value)
                    } catch {
                        case e: Exception =>
                            log.error("decrByWithNoReply exception key=%s,value=%s".format(key, value))
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("redis back queue is full in decrByWithNoReply")
        }

    }

    def sget(key: String): String = {

        var jedis: Jedis = null
        try {

            jedis = getJedis()
            val value = jedis.smembers(key)
            freeJedis(jedis)

            if (value == null || value.size == 0) return null
            val buff = new StringBuilder
            val i = value.iterator()
            while (i.hasNext()) {
                val v = i.next()
                if (buff.length > 0) { buff.append("$") }
                buff.append(v)
            }
            buff.toString

        } catch {
            case e: Throwable =>
                if (jedis != null) {
                    freeBorkenJedis(jedis)
                }
                throw e
        }
    }

    def sadd(key: String, exp: Int, value: String): Boolean = {

        var jedis: Jedis = null
        try {

            jedis = getJedis()
            var exist = false
            if (exp > 0) {
                exist = jedis.exists(key)
            }
            val s = jedis.sadd(key, value)
            if (exp > 0 && !exist) { // set expire only first time
                jedis.expire(key, exp)
            }
            freeJedis(jedis)
            val cnt = errorCount.intValue()
            if (cnt >= 1) errorCount.decrementAndGet()
            return (s == 0 || s == 1)
        } catch {
            case e: Throwable =>
                if (jedis != null) {
                    freeBorkenJedis(jedis)
                }
                val cnt = errorCount.incrementAndGet()
                if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                log.error(tag + " sadd exception key=%s,value=%s".format(key, value))
                throw e
        }

    }

    def saddWithNoReply(key: String, exp: Int, value: String) {
        try {
            backthread.execute(new Runnable() {
                def run() {
                    try {
                        sadd(key, exp, value)
                    } catch {
                        case e: Exception =>
                            log.error(tag + " saddWithNoReply exception key=%s,value=%s".format(key, value))
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("redis back queue is full in saddWithNoReply")
        }

    }

    def sremove(key: String, exp: Int, value: String): Boolean = {

        var jedis: Jedis = null
        try {

            jedis = getJedis()
            val s = jedis.srem(key, value)
            freeJedis(jedis)
            val cnt = errorCount.intValue()
            if (cnt >= 1) errorCount.decrementAndGet()
            return (s == 0 || s == 1)
        } catch {
            case e: Throwable =>
                if (jedis != null) {
                    freeBorkenJedis(jedis)
                }
                val cnt = errorCount.incrementAndGet()
                if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                log.error(tag + " sremove exception key=%s,value=%s".format(key, value))
                throw e
        }

    }

    def sremoveWithNoReply(key: String, exp: Int, value: String) {
        try {
            backthread.execute(new Runnable() {
                def run() {
                    try {
                        sremove(key, exp, value)
                    } catch {
                        case e: Exception =>
                            log.error(tag + " sremoveWithNoReply exception key=%s,value=%s".format(key, value))
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("redis back queue is full in sremoveWithNoReply")
        }

    }

    def getJedis(): Jedis = {
        pool.getResource()
    }

    def freeJedis(jedis: BinaryJedis): Unit = {
        pool.returnResource(jedis)
    }

    def freeBorkenJedis(jedis: BinaryJedis): Unit = {
        pool.returnBrokenResource(jedis)
    }

    def destroy() {
        try {
            pool.destroy()
        } catch {
            case e: Exception =>
                log.error("destroy exception e={}", e.getMessage)
        }
    }

}

/*
class SharedJedisPoolWrapper(val pool : ShardedJedisPool, val backthread : ThreadPoolExecutor) extends Logging {

    val SELFTESTKEY = "selftest_redis"

    def hasError() : Boolean = {
        try {
            set(SELFTESTKEY,180,"OK") // 3 minutes
            return false
        } catch {
            case e:Throwable =>
                return true
        }
    }

    def get(key:String) :String = {

        var jedis : ShardedJedis = null
        try {

            jedis = getJedis()
            val value = jedis.get(key)
            freeJedis(jedis)
            value

        } catch {
            case e:Throwable =>
                if( jedis != null ) {
                    freeBorkenJedis(jedis)
                }
                throw e
        }
    }

    def set(key:String,exp:Int,value:String) : Boolean =  {

        var jedis : ShardedJedis = null
        try {

            jedis = getJedis()
            val s = jedis.setex(key, exp ,value)
            freeJedis(jedis)
            return (s == "OK")

        } catch {
            case e:Throwable =>
                if( jedis != null ) {
                    freeBorkenJedis(jedis)
                }
                log.error("set exception key=%s,value=%s".format(key,value))
                throw e
        }

    }

    def delete(key:String)  : Boolean = {

        var jedis : ShardedJedis = null
        try {

            jedis = getJedis()
            val cnt = jedis.del(key)
            freeJedis(jedis)
            return ( cnt > 0 )
        } catch {
            case e:Throwable =>
                if( jedis != null ) {
                    freeBorkenJedis(jedis)
                }
                log.error("delete exception key=%s".format(key))
                throw e
        }

    }

    def incrBy(key:String,exp:Int,value:Long) : Long =  {

        var jedis : ShardedJedis = null
        try {

            jedis = getJedis()
            val newvalue = jedis.incrBy(key, value)
            if( newvalue == value ) {
                jedis.expire(key,exp)
            }
            freeJedis(jedis)
            return newvalue

        } catch {
            case e:Throwable =>
                if( jedis != null ) {
                    freeBorkenJedis(jedis)
                }
                log.error("incr exception key=%s,value=%s".format(key,value))
                throw e
        }

    }

    def decrBy(key:String,exp:Int,value:Long) : Long =  {

        var jedis : ShardedJedis = null
        try {

            jedis = getJedis()
            val newvalue = jedis.decrBy(key, value)
            if( newvalue == ( value * -1) ) {
                jedis.expire(key,exp)
            }
            freeJedis(jedis)
            return newvalue

        } catch {
            case e:Throwable =>
                if( jedis != null ) {
                    freeBorkenJedis(jedis)
                }
                log.error("decr exception key=%s,value=%s".format(key,value))
                throw e
        }

    }

    def getJedis() : ShardedJedis = {
        pool.getResource()
    }

    def freeJedis(jedis: ShardedJedis) : Unit = {
        pool.returnResource(jedis)
    }

    def freeBorkenJedis(jedis: ShardedJedis) : Unit = {
        pool.returnBrokenResource(jedis)
    }

    def destroy() {
        try {
            pool.destroy()
        } catch {
            case e:Exception =>
                log.error("destroy exception e={}",e.getMessage)
        }
    }

}

var pool_shared :  SharedJedisPoolWrapper  = _

            case RemoteCacheLike.TYPE_CONHASH =>

                val list = new java.util.ArrayList[JedisShardInfo]()

                for( addr <- serverAddrs ) {
                    val ss = addr.split(":")
                    val ip = ss(0)
                    val port = ss(1).toInt
                    val info = new JedisShardInfo(ip, port, timeout)
                    list.add(info)
                }
                pool_shared = new SharedJedisPoolWrapper( new ShardedJedisPool(config, list, Hashing.MURMUR_HASH),backthread )

                if( pool_shared != null ) {
                    pool_shared.destroy()
                    pool_shared = null
                }
*/

class RedisClient(override val cacheTypeCfg: Int,
                  override val serverAddrs: List[String],
                  override val serviceIds: String,
                  override val readThreadNum: Int,
                  override val writeThreadNum: Int,
                  override val router: Router,
                  override val cfgNode: Node)
        extends BaseCacheClient(cacheTypeCfg, serverAddrs, serviceIds, readThreadNum, writeThreadNum, router, cfgNode) with Dumpable {

    var pools = new ArrayBuffer[JedisPoolWrapper]()
    var poolFailOverIdx = new ArrayBufferInt()
    var treeMap: TreeMap[Long, Int] = _

    var pool_master: JedisPoolWrapper = _
    var pool_slave: JedisPoolWrapper = _
    var useMaster = new AtomicBoolean(true)
    var maxErrorCount = 50
    var failOver = true

    var threadFactory: ThreadFactory = _
    var backthread: ThreadPoolExecutor = _
    var timer: Timer = _

    init

    def dump() {
        if (backthread != null) {
            val buff = new StringBuilder
            buff.append("backthread.size=").append(backthread.getPoolSize).append(",")
            buff.append("backthread.getQueue.size=").append(backthread.getQueue.size).append(",")
            log.info(buff.toString)
        }
    }

    def createPool(config: JedisPoolConfig, addr: String): JedisPool = {
        val ss = addr.split(":")
        val ip = ss(0)
        val port = ss(1).toInt

        val p = new JedisPool(config, ip, port, timeout)
        p
    }

    def getPool(key: String): JedisPoolWrapper = {
        val h = Hashing.MURMUR_HASH.hash(key)

        var idx = -1
        if (cacheType == RemoteCacheLike.TYPE_ARRAYHASH) {
            idx = (h % pools.size).toInt
            if (idx < 0) idx = -1 * idx
        } else {
            val sortedMap = treeMap.tailMap(h)
            if (sortedMap == null || sortedMap.isEmpty)
                idx = treeMap.get(treeMap.firstKey).toInt
            else
                idx = treeMap.get(sortedMap.firstKey).toInt
        }
        // println("idx="+idx)

        if (!failOver) return pools(idx)

        val realIdx = poolFailOverIdx(idx)
        var pool = pools(realIdx)
        val cnt = pool.errorCount.intValue()
        if (cnt < maxErrorCount) return pool

        for (i <- 0 until pools.size) {
            val tidx = (idx + i) % pools.size
            if (tidx != realIdx) {
                pool = pools(tidx)
                val cnt = pool.errorCount.intValue()
                if (cnt == 0) {
                    poolFailOverIdx(idx) = tidx
                    log.error("switch from [" + pools(idx).tag + "/" + pools(realIdx).tag + "] to " + pools(tidx).tag)
                    return pool
                }
            }
        }

        pools(realIdx)
    }

    def recover() {

        for (idx <- 0 until pools.size) {
            val realIdx = poolFailOverIdx(idx)
            if (idx != realIdx) {
                val pool = pools(idx)
                var hasError = false
                var k = 0
                while (k < (maxErrorCount + 1) && !hasError) {
                    hasError = pool.hasError()
                    k += 1
                }
                val cnt = pool.errorCount.intValue()
                if (!hasError && cnt == 0) {
                    poolFailOverIdx(idx) = idx
                    log.error("switch by timer from [" + pools(idx).tag + "/" + pools(realIdx).tag + "] to " + pools(idx).tag)
                }
            }
        }

    }

    override def init() {

        super.init()

        var s = (cfgNode \ "@errorCountToSwitch").text
        if (s != "") {
            maxErrorCount = s.toInt
        }
        //println("maxErrorCount="+maxErrorCount)
        s = (cfgNode \ "@failOver").text
        if (s != "") {
            failOver = (s == "true" || s == "yes" || s == "t" || s == "y" || s == "1")
        }
        //println("failOver="+failOver)

        val firstServiceId = serviceIds.split(",")(0)
        if (cacheType == RemoteCacheLike.TYPE_MASTERSLAVE) {
            threadFactory = new NamedThreadFactory("redisback" + firstServiceId)
            backthread = new ThreadPoolExecutor(writeThreadNum, writeThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](10000), threadFactory)
            backthread.prestartAllCoreThreads()
        }
        log.info("cacheType=" + cacheType + ", serviceId=" + firstServiceId)

        val totalThreadNum = readThreadNum + writeThreadNum

        val config = new JedisPoolConfig()

        config.setMaxActive(totalThreadNum)
        config.setMaxIdle(totalThreadNum)
        config.setMinIdle(totalThreadNum)
        config.setMaxWait(3000)
        config.setTestOnBorrow(false)
        config.setTestOnReturn(false)
        config.setTestWhileIdle(false)

        //config.setTimeBetweenEvictionRunsMillis(60000); // 1分钟检查一次
        //config.setTimeBetweenEvictionRunsMillis(-1); // 不检查
        //config.setNumTestsPerEvictionRun(minIdle); // 每次检查数量.
        config.setMinEvictableIdleTimeMillis(500 * 24 * 60 * 60 * 1000L); // 改成500天idle才会释放重连

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                for (i <- 0 until serverAddrs.size) {
                    val pool = new JedisPoolWrapper(serverAddrs(i), createPool(config, serverAddrs(i)), backthread, maxErrorCount)
                    pools += pool
                    poolFailOverIdx += i
                }
                if (cacheType == RemoteCacheLike.TYPE_CONHASH) {
                    treeMap = new TreeMap[Long, Int]()
                    for (i <- 0 until serverAddrs.size) {
                        for (j <- 0 until 50) {
                            val h = Hashing.MURMUR_HASH.hash(serverAddrs(i) + "-" + i + "-" + j)
                            treeMap.put(h, i)
                        }
                    }
                }

                timer = new Timer("redisrecovertimer")
                timer.schedule(new TimerTask() {
                    def run() {
                        recover()
                    }
                }, 10000, 10000)

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                pool_master = new JedisPoolWrapper(serverAddrs(0), createPool(config, serverAddrs(0)), backthread, maxErrorCount)
                pool_slave = new JedisPoolWrapper(serverAddrs(1), createPool(config, serverAddrs(1)), backthread, maxErrorCount)
        }

    }

    def close() {

        if (timer != null) {
            timer.cancel()
            timer = null
        }
        if (backthread != null) {
            val t1 = System.currentTimeMillis

            backthread.shutdown()

            backthread.awaitTermination(5, TimeUnit.SECONDS)

            val t2 = System.currentTimeMillis
            if (t2 - t1 > 100)
                log.warn("RedisClient long time to shutdown backthread, ts={}", t2 - t1)
        }

        for (pool <- pools) {
            pool.destroy()
        }
        pools.clear()

        if (pool_master != null) {
            pool_master.destroy()
            pool_master = null
        }

        if (pool_slave != null) {
            pool_slave.destroy()
            pool_slave = null
        }
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301005

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                var i = 0
                while (i < pools.size) {

                    if (pools(i).hasError()) {
                        val msg = "redis [" + serverAddrs(i) + "] has error"
                        buff += new SelfCheckResult("SCALABPE.REDIS", errorId, true, msg)
                    }

                    i += 1
                }

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                if (pool_master.hasError()) {
                    val msg = "master redis [" + serverAddrs(0) + "] has error"
                    buff += new SelfCheckResult("SCALABPE.REDIS", errorId, true, msg)
                }

                if (pool_slave.hasError()) {
                    val msg = "slave redis [" + serverAddrs(1) + "] has error"
                    buff += new SelfCheckResult("SCALABPE.REDIS", errorId, true, msg)
                }

        }

        if (buff.size == 0) {
            buff += new SelfCheckResult("SCALABPE.REDIS", errorId)
        }

        buff
    }

    def getFirstSecond(): Tuple3[JedisPoolWrapper, JedisPoolWrapper, Boolean] = {

        val master_cnt = pool_master.errorCount.intValue()
        val slave_cnt = pool_slave.errorCount.intValue()
        // println("master/slave:%d/%d".format(master_cnt,slave_cnt))
        if (useMaster.get()) {
            if (master_cnt >= maxErrorCount && slave_cnt <= 0) {
                log.error("switch to slave, addrs=" + serverAddrs.toString)
                useMaster.set(false)
                (pool_slave, pool_master, master_cnt < maxErrorCount)
            } else {
                (pool_master, pool_slave, slave_cnt < maxErrorCount)
            }
        } else {
            if (slave_cnt >= maxErrorCount && master_cnt <= 0) {
                log.error("switch to master, addrs=" + serverAddrs.toString)
                useMaster.set(true)
                (pool_master, pool_slave, slave_cnt < maxErrorCount)
            } else {
                (pool_slave, pool_master, master_cnt < maxErrorCount)
            }
        }
    }

    def get(key: String): String = {

        var v: String = null

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val pool = getPool(key)
                v = pool.get(key)

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                // println("secondValid="+secondValid)
                try {
                    v = first.get(key)
                } catch {
                    case e: Exception =>
                        v = second.get(key)
                        return v
                }
                if (v == null && secondValid) {
                    v = second.get(key)
                }

        }
        v
    }

    def set(key: String, exp: Int, value: String): Boolean = {

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val pool = getPool(key)
                val ok = pool.set(key, exp, value)
                return ok

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                var ok = false
                try {
                    ok = first.set(key, exp, value)
                    second.setWithNoReply(key, exp, value)
                } catch {
                    case e: Exception =>
                        return second.set(key, exp, value)
                }

                return ok
        }

        false
    }

    def delete(key: String): Boolean = {

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val pool = getPool(key)
                val ok = pool.delete(key)
                return ok

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                var ok = false
                try {
                    ok = first.delete(key)
                    second.deleteWithNoReply(key)
                } catch {
                    case e: Exception =>
                        return second.delete(key)
                }
                return ok
        }

        false
    }

    def getAndDelete(key: String): String = {

        var v = get(key)
        if (v != null) {

            cacheType match {

                case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                    val pool = getPool(key)
                    pool.delete(key)

                case RemoteCacheLike.TYPE_MASTERSLAVE =>

                    val first = pool_master
                    val second = pool_slave

                    first.deleteWithNoReply(key)
                    second.deleteWithNoReply(key)

            }

        }
        v
    }

    def incrBy(key: String, exp: Int, value: String): String = {

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val pool = getPool(key)
                val newvalue = pool.incrBy(key, exp, value.toLong)
                return newvalue.toString

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                try {
                    val newvalue = first.incrBy(key, exp, value.toLong)
                    second.setWithNoReply(key, exp, newvalue.toString) // use set not incr
                    return newvalue.toString
                } catch {
                    case e: Exception =>
                        val newvalue = second.incrBy(key, exp, value.toLong)
                        return newvalue.toString
                }
        }

        null
    }

    def decrBy(key: String, exp: Int, value: String): String = {

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val pool = getPool(key)
                val newvalue = pool.decrBy(key, exp, value.toLong)
                return newvalue.toString

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                try {
                    val newvalue = first.decrBy(key, exp, value.toLong)
                    second.setWithNoReply(key, exp, newvalue.toString) // use set not decr
                    return newvalue.toString
                } catch {
                    case e: Exception =>
                        val newvalue = second.decrBy(key, exp, value.toLong)
                        return newvalue.toString
                }
        }

        null
    }

    def getAndCas(key: String, exp: Int, addValue: String): String = {
        throw new RuntimeException("getandcas not supported")
    }

    def sget(key: String): String = {

        var v: String = null

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val pool = getPool(key)
                v = pool.sget(key)

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                // println("secondValid="+secondValid)
                try {
                    v = first.sget(key)
                } catch {
                    case e: Exception =>
                        v = second.sget(key)
                        return v
                }
                if (v == null && secondValid) {
                    v = second.sget(key)
                }

        }
        v
    }

    def sadd(key: String, exp: Int, value: String): Boolean = {

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val pool = getPool(key)
                val ok = pool.sadd(key, exp, value)
                return ok

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                var ok = false
                try {
                    ok = first.sadd(key, exp, value)
                    second.saddWithNoReply(key, exp, value)
                } catch {
                    case e: Exception =>
                        return second.sadd(key, exp, value)
                }

                return ok
        }

        false
    }

    def sremove(key: String, exp: Int, value: String): Boolean = {

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val pool = getPool(key)
                val ok = pool.sremove(key, exp, value)
                return ok

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                var ok = false
                try {
                    ok = first.sremove(key, exp, value)
                    second.sremoveWithNoReply(key, exp, value)
                } catch {
                    case e: Exception =>
                        return second.sremove(key, exp, value)
                }

                return ok
        }

        false
    }
}

