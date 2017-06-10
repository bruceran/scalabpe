package scalabpe.plugin

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.xml.Node

import net.rubyeye.xmemcached.GetsResponse
import net.rubyeye.xmemcached.MemcachedClient
import net.rubyeye.xmemcached.XMemcachedClientBuilder
import net.rubyeye.xmemcached.command.TextCommandFactory
import net.rubyeye.xmemcached.impl.ArrayMemcachedSessionLocator
import net.rubyeye.xmemcached.impl.KetamaMemcachedSessionLocator
import net.rubyeye.xmemcached.transcoders.StringTranscoder
import net.rubyeye.xmemcached.utils.AddrUtil
import scalabpe.core.Dumpable
import scalabpe.core.Logging
import scalabpe.core.NamedThreadFactory
import scalabpe.core.Router
import scalabpe.core.SelfCheckLike
import scalabpe.core.SelfCheckResult

class MemCacheActor(override val router: Router, override val cfgNode: Node)
        extends BaseCacheActor(router, cfgNode) with SelfCheckLike {

    init

    override def init() {
        super.init()
        cache = new MemCacheClient(cacheType, addrs, serviceIds, readThreadNum, writeThreadNum, router, cfgNode)
        log.info("MemCachedActor started {}", serviceIds)
    }

    override def close() {
        super.close()
        cache.asInstanceOf[MemCacheClient].close()
        log.info("MemCachedActor stopped {}", serviceIds)
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {
        val buff = cache.asInstanceOf[MemCacheClient].selfcheck()
        buff
    }

}

class MemCacheWrapper(val tag: String, val c: MemcachedClient, val backthread: ThreadPoolExecutor, val timeout: Int, val maxErrorCount: Int) extends Logging {

    val errorCount = new AtomicInteger(0)

    val SELFTESTKEY = "selftest_memcached"

    def hasErrorForAll(size: Int): Boolean = {
        try {

            val stats = c.getStats(3000)

            /*

            stats: java.util.Map[java.net.InetSocketAddress,java.util.Map[String,String]] = {/10.241.37.37:11211={delete_hits=0, bytes=0, total_items=0, rusage_system=0.625904, listen_disabled_num=0, auth_errors=0, evictions=0, version=1.4.5, pointer_size=32, time=1384157510, incr_hits=0, threads=4, limit_maxbytes=1073741824, bytes_read=529382, curr_connections=11, get_misses=0, reclaimed=0, bytes_written=883071, connection_structures=12, cas_hits=0, delete_misses=0, total_connections=57, cmd_flush=0, rusage_user=0.300954, uptime=968122, pid=7634, cas_badval=0, get_hits=0, curr_items=0, cas_misses=0, accepting_conns=1, cmd_get=0, cmd_set=0, incr_misses=0, auth_cmds=0, decr_misses=0, decr_hits=0, conn_yields=0}}

            if( log.isDebugEnabled() ) {
                val entrylist = stats.entrySet().iterator()
                while( entrylist.hasNext() ) {
                    val entry = entrylist.next()
                    log.debug(entry.getKey.toString + "=" + entry.getValue.toString)
                }

            }

             */

            if (stats == null || stats.size != size) {
                return true
            }

            return false

        } catch {
            case e: Throwable =>
                return true
        }
    }

    def hasError(): Boolean = {
        try {
            val ok = c.set(SELFTESTKEY, 180, "OK") // 3 minutes
            return !ok
        } catch {
            case e: Throwable =>
                return true
        }
    }

    def get(key: String): String = {
        return c.get(key)
    }

    def adjustExp(exp: Int): Int = {
        if (exp <= 30 * 24 * 60 * 60) return exp // supported by memcached natively
        val now = (System.currentTimeMillis() / 1000).toInt
        if (exp > now) return exp // an absolute date
        now + exp // generate date by exp
    }

    def set(key: String, exp: Int, value: String): Boolean = {
        try {
            val ok = c.set(key, adjustExp(exp), value, timeout)
            val cnt = errorCount.intValue()
            if (cnt >= 1) errorCount.decrementAndGet()
            return ok
        } catch {
            case e: Exception =>
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
                        c.set(key, adjustExp(exp), value, timeout)
                        val cnt = errorCount.intValue()
                        if (cnt >= 1) errorCount.decrementAndGet()
                    } catch {
                        case e: Exception =>
                            val cnt = errorCount.incrementAndGet()
                            if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                            log.error(tag + " setWithNoReply exception key=%s,value=%s".format(key, value))
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("memcache back queue is full in setWithNoReply")
        }
    }

    def delete(key: String): Boolean = {
        try {
            val ok = c.delete(key)
            val cnt = errorCount.intValue()
            if (cnt >= 1) errorCount.decrementAndGet()
            return ok
        } catch {
            case e: Exception =>
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
                        c.delete(key)
                        val cnt = errorCount.intValue()
                        if (cnt >= 1) errorCount.decrementAndGet()
                    } catch {
                        case e: Exception =>
                            val cnt = errorCount.incrementAndGet()
                            if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                            log.error(tag + " deleteWithNoReply exception key=%s".format(key))
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("memcache back queue is full in deleteWithNoReply")
        }
    }

    def incrBy(key: String, exp: Int, value: String): String = {
        try {
            val newvalue = c.incr(key, value.toLong, value.toLong, timeout, adjustExp(exp))
            val cnt = errorCount.intValue()
            if (cnt >= 1) errorCount.decrementAndGet()
            return newvalue.toString
        } catch {
            case e: Exception =>
                val cnt = errorCount.incrementAndGet()
                if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                log.error(tag + " incrBy exception key=%s,value=%s".format(key, value))
                throw e
        }
    }

    def incrByWithNoReply(key: String, exp: Int, value: String) {
        try {
            backthread.execute(new Runnable() {
                def run() {
                    try {
                        c.incr(key, value.toLong, value.toLong, timeout, adjustExp(exp))
                        val cnt = errorCount.intValue()
                        if (cnt >= 1) errorCount.decrementAndGet()
                    } catch {
                        case e: Exception =>
                            val cnt = errorCount.incrementAndGet()
                            if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                            log.error(tag + " incrByWithNoReply exception key=%s,value=%s".format(key, value))
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("memcache back queue is full in incrByWithNoReply")
        }
    }

    // memcached incr/decr value is >=0
    def decrBy(key: String, exp: Int, value: String): String = {
        try {
            //val newvalue = c.decr(key,value.toLong,0,timeout,adjustExp(exp))
            val newvalue = c.decr(key, value.toLong, 0, timeout)
            val cnt = errorCount.intValue()
            if (cnt >= 1) errorCount.decrementAndGet()
            return newvalue.toString
        } catch {
            case e: Exception =>
                val cnt = errorCount.incrementAndGet()
                if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                log.error(tag + " decrBy exception key=%s,value=%s".format(key, value))
                throw e
        }
    }

    def decrByWithNoReply(key: String, exp: Int, value: String) {
        try {
            backthread.execute(new Runnable() {
                def run() {
                    try {
                        //c.decr(key,value.toLong,0,timeout,adjustExp(exp))
                        c.decr(key, value.toLong, 0, timeout)
                        val cnt = errorCount.intValue()
                        if (cnt >= 1) errorCount.decrementAndGet()
                    } catch {
                        case e: Exception =>
                            val cnt = errorCount.incrementAndGet()
                            if (cnt >= maxErrorCount) errorCount.set(maxErrorCount)
                            log.error(tag + " decrByWithNoReply exception key=%s,value=%s".format(key, value))
                    }
                }
            })
        } catch {
            case e: RejectedExecutionException =>
                log.error("memcache back queue is full in decrByWithNoReply")
        }

    }

    def destroy() {
        try {
            c.shutdown()
        } catch {
            case e: Exception =>
                log.error("destroy exception e={}", e.getMessage)
        }
    }

}

class MemCacheClient(override val cacheTypeCfg: Int,
                     override val serverAddrs: List[String],
                     override val serviceIds: String,
                     override val readThreadNum: Int,
                     override val writeThreadNum: Int,
                     override val router: Router,
                     override val cfgNode: Node) extends BaseCacheClient(cacheTypeCfg, serverAddrs, serviceIds, readThreadNum, writeThreadNum, router, cfgNode) with Dumpable {

    val casRetryWaitTime = 25
    val casRetryTimes = 3

    var xmc: MemCacheWrapper = _
    var failOver = true

    var xmc_master: MemCacheWrapper = _
    var xmc_slave: MemCacheWrapper = _
    var useMaster = new AtomicBoolean(true)
    var maxErrorCount = 50

    var threadFactory: ThreadFactory = _
    var backthread: ThreadPoolExecutor = _

    init

    def dump() {
        if (backthread != null) {
            val buff = new StringBuilder
            buff.append("backthread.size=").append(backthread.getPoolSize).append(",")
            buff.append("backthread.getQueue.size=").append(backthread.getQueue.size).append(",")
            log.info(buff.toString)
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
            threadFactory = new NamedThreadFactory("cacheback" + firstServiceId)
            backthread = new ThreadPoolExecutor(writeThreadNum, writeThreadNum, 0, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](10000), threadFactory)
            backthread.prestartAllCoreThreads()
        }
        log.info("cacheType=" + cacheType + ", serviceId=" + firstServiceId)

        val totalThreadNum = readThreadNum + writeThreadNum

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH =>

                val addrs = serverAddrs.mkString(" ")
                val builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(addrs));
                builder.setCommandFactory(new TextCommandFactory());
                builder.setSessionLocator(new ArrayMemcachedSessionLocator())
                builder.setTranscoder(new StringTranscoder("UTF-8"))
                builder.setConnectionPoolSize(totalThreadNum); // CONFIG
                if (!failOver) builder.setFailureMode(true);
                xmc = new MemCacheWrapper("", builder.build(), backthread, timeout, maxErrorCount)

            case RemoteCacheLike.TYPE_CONHASH =>

                val addrs = serverAddrs.mkString(" ")
                val builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(addrs));
                builder.setCommandFactory(new TextCommandFactory());
                builder.setSessionLocator(new KetamaMemcachedSessionLocator())
                builder.setTranscoder(new StringTranscoder("UTF-8"))
                builder.setConnectionPoolSize(totalThreadNum); // CONFIG
                if (!failOver) builder.setFailureMode(true);
                xmc = new MemCacheWrapper("", builder.build(), backthread, timeout, maxErrorCount)

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val builder1 = new XMemcachedClientBuilder(AddrUtil.getAddresses(serverAddrs(0)));
                builder1.setCommandFactory(new TextCommandFactory());
                builder1.setTranscoder(new StringTranscoder("UTF-8"))
                builder1.setConnectionPoolSize(totalThreadNum); // CONFIG
                builder1.setFailureMode(true);
                xmc_master = new MemCacheWrapper(serverAddrs(0), builder1.build(), backthread, timeout, maxErrorCount)

                val builder2 = new XMemcachedClientBuilder(AddrUtil.getAddresses(serverAddrs(1)));
                builder2.setCommandFactory(new TextCommandFactory());
                builder2.setTranscoder(new StringTranscoder("UTF-8"))
                builder2.setConnectionPoolSize(totalThreadNum); // CONFIG
                builder2.setFailureMode(true);
                xmc_slave = new MemCacheWrapper(serverAddrs(1), builder2.build(), backthread, timeout, maxErrorCount)
        }

    }

    def close() {

        if (backthread != null) {
            val t1 = System.currentTimeMillis

            backthread.shutdown()

            backthread.awaitTermination(5, TimeUnit.SECONDS)

            val t2 = System.currentTimeMillis
            if (t2 - t1 > 100)
                log.warn("memcacheclient long time to shutdown backthread, ts={}", t2 - t1)
        }

        if (xmc != null) {
            xmc.destroy()
            xmc = null
        }
        if (xmc_master != null) {
            xmc_master.destroy()
            xmc_master = null
        }
        if (xmc_slave != null) {
            xmc_slave.destroy()
            xmc_slave = null
        }
    }

    def selfcheck(): ArrayBuffer[SelfCheckResult] = {

        val buff = new ArrayBuffer[SelfCheckResult]()

        var errorId = 65301004

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                if (xmc.hasErrorForAll(serverAddrs.size)) {
                    val msg = "cache [" + serverAddrs.mkString(",") + "] has error"
                    buff += new SelfCheckResult("SCALABPE.CACHE", errorId, true, msg)
                }

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                if (xmc_master.hasError()) {
                    val msg = "master cache [" + serverAddrs(0) + "] has error"
                    buff += new SelfCheckResult("SCALABPE.CACHE", errorId, true, msg)
                }

                if (xmc_slave.hasError()) {
                    val msg = "slave cache [" + serverAddrs(1) + "] has error"
                    buff += new SelfCheckResult("SCALABPE.CACHE", errorId, true, msg)
                }
        }

        if (buff.size == 0) {
            buff += new SelfCheckResult("SCALABPE.CACHE", errorId)
        }

        buff
    }

    def getFirstSecond(): Tuple3[MemCacheWrapper, MemCacheWrapper, Boolean] = {

        val master_cnt = xmc_master.errorCount.intValue()
        val slave_cnt = xmc_slave.errorCount.intValue()
        // println("master/slave:%d/%d".format(master_cnt,slave_cnt))
        if (useMaster.get()) {
            if (master_cnt >= maxErrorCount && slave_cnt <= 0) {
                log.error("switch to slave, addrs=" + serverAddrs.toString)
                useMaster.set(false)
                (xmc_slave, xmc_master, master_cnt < maxErrorCount)
            } else {
                (xmc_master, xmc_slave, slave_cnt < maxErrorCount)
            }
        } else {
            if (slave_cnt >= maxErrorCount && master_cnt <= 0) {
                log.error("switch to master, addrs=" + serverAddrs.toString)
                useMaster.set(true)
                (xmc_master, xmc_slave, slave_cnt < maxErrorCount)
            } else {
                (xmc_slave, xmc_master, master_cnt < maxErrorCount)
            }
        }
    }

    def get(key: String): String = {

        var v: String = null

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                v = xmc.get(key)

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                // println("secondValid="+secondValid)
                try {
                    v = first.get(key)
                } catch {
                    case e: Exception =>
                        v = second.get(key)
                        if (v != null) v = v.trim
                        return v
                }
                if (v == null && secondValid) {
                    v = second.get(key)
                }

        }
        if (v != null) v = v.trim // blank spaces may be appended to number during decr operation
        v
    }

    def set(key: String, exp: Int, value: String): Boolean = {

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val ok = xmc.set(key, exp, value)
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

                val ok = xmc.delete(key)
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

                    xmc.delete(key)

                case RemoteCacheLike.TYPE_MASTERSLAVE =>

                    val first = xmc_master
                    val second = xmc_slave

                    first.deleteWithNoReply(key)
                    second.deleteWithNoReply(key)

            }

        }
        v
    }

    def incrBy(key: String, exp: Int, value: String): String = {

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val newvalue = xmc.incrBy(key, exp, value)
                return newvalue

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                try {
                    val newvalue = first.incrBy(key, exp, value)
                    second.setWithNoReply(key, exp, newvalue) // use set not incr
                    return newvalue
                } catch {
                    case e: Exception =>
                        val newvalue = second.incrBy(key, exp, value)
                        return newvalue
                }
        }

        null
    }

    def decrBy(key: String, exp: Int, value: String): String = {

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                val newvalue = xmc.decrBy(key, exp, value)
                return newvalue

            case RemoteCacheLike.TYPE_MASTERSLAVE =>

                val (first, second, secondValid) = getFirstSecond()

                try {
                    val newvalue = first.decrBy(key, exp, value)
                    second.setWithNoReply(key, exp, newvalue) // use set not decr
                    return newvalue
                } catch {
                    case e: Exception =>
                        val newvalue = second.decrBy(key, exp, value)
                        return newvalue
                }
        }

        null
    }

    def getAndCas(key: String, exp: Int, addValue: String): String = {

        cacheType match {

            case RemoteCacheLike.TYPE_ARRAYHASH | RemoteCacheLike.TYPE_CONHASH =>

                return getAndCas(xmc.c, key, exp, addValue)

            case _ =>

                throw new RuntimeException("masterslave not support getandcas")

        }

    }

    def getAndCas(c: MemcachedClient, key: String, exp: Int, addValue: String): String = {

        var retryii = 0
        while (retryii < casRetryTimes) {

            val res: GetsResponse[String] = c.gets(key, timeout)
            if (res == null) {

                val ok = c.set(key, exp, addValue, timeout)
                if (ok) return addValue

            } else {
                val cas = res.getCas

                val addValues = splitValue(addValue)
                var oldValues = splitValue(res.getValue)

                val newValues = new ArrayBuffer[Int]()

                try {
                    var i = 0
                    while (i < addValues.size) {
                        newValues += (oldValues(i).toInt + addValues(i).toInt)
                        i += 1
                    }
                } catch {
                    case e: Exception =>
                        log.error("getAndCas value not valid, reset value to addValue")
                        val ok = c.set(key, exp, addValue, timeout)
                        if (ok) return addValue
                }

                val newValue = genValueOfInts(newValues)

                val ok = c.cas(key, exp, newValue, timeout, cas)
                if (ok) return newValue

            }

            Thread.sleep(casRetryWaitTime)

            retryii += 1
        }

        throw new RuntimeException("getandcas failed after retry")
    }

    def sget(key: String): String = {
        throw new RuntimeException("sget not support")
    }
    def sadd(key: String, exp: Int, value: String): Boolean = {
        throw new RuntimeException("sadd not support")
    }
    def sremove(key: String, exp: Int, value: String): Boolean = {
        throw new RuntimeException("sremove not support")
    }
}

