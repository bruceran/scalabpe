package scalabpe.core

import java.io.File
import java.net.InetAddress
import java.net.NetworkInterface
import java.security.Key
import java.security.MessageDigest
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import org.apache.commons.io.FileUtils

import javax.crypto.Cipher
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.DESKeySpec

object IpUtils extends Logging {

    val localips = ArrayBufferString()
    val netips = ArrayBufferString()

    loadIps()

    def loadIps() {

        try {
            val netInterfaces = NetworkInterface.getNetworkInterfaces();
            while (netInterfaces.hasMoreElements()) {
                val address = netInterfaces.nextElement().getInetAddresses();
                while (address.hasMoreElements()) {
                    val ip = address.nextElement()

                    if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":") == -1) {
                        netips += ip.getHostAddress()
                    } else if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":") == -1) {
                        localips += ip.getHostAddress()
                    }
                }
            }
        } catch {
            case e: Exception =>
        }

        //println("localips="+localips.mkString(","))
        //println("netips="+netips.mkString(","))

    }

    def localIp0(): String = {

        try {
            val addr = InetAddress.getLocalHost()
            addr.getHostAddress()
        } catch {
            case e: Exception => "127.0.0.1"
        }

    }

    def localIp(): String = {

        if (true) {
            val envhost = System.getenv("SCALABPE_HOST") // used in docker
            if (envhost != null && envhost != "") {
                try {
                    val addr = InetAddress.getByName(envhost)
                    val s = addr.getHostAddress()
                    return s
                } catch {
                    case e: Exception =>
                        log.error("cannot get host address, use local ip")
                }
            }
        }

        val docker0 = "172.17.0.1"
        localips -= docker0

        val ip0 = localIp0() // 此ip可通过调整/etc/hosts文件改变

        if (localips.size > 0) {
            if (localips.contains(ip0)) return ip0
            return localips(0)
        }

        if (netips.size > 0) {
            if (netips.contains(ip0)) return ip0
            return netips(0)
        }

        ip0
    }

    def publicIp(): String = {
        if (netips.size > 0) {
            return netips(0)
        }
        localIp()
    }

    def serverId(): String = {
        val s = localIp();
        val ss = s.split("\\.");
        val t = "%03d%03d".format(ss(2).toInt, ss(3).toInt)
        t
    }

}

object RequestIdGenerator {

    var savedTime = 0L
    var savedIndex = 10000
    val lock = new ReentrantLock(false)
    val serverId = IpUtils.serverId

    def nextId(): String = {

        var now = 0L
        var index = 0

        lock.lock();

        try {
            now = System.currentTimeMillis();
            if (now == savedTime) {
                savedIndex += 1;
            } else {
                savedTime = now;
                savedIndex = 10000;
            }

            index = savedIndex;
        } finally {
            lock.unlock();
        }

        serverId + now + index.toString
    }
}

class NamedThreadFactory(val namePrefix: String) extends ThreadFactory {

    val threadNumber = new AtomicInteger(1)
    val s = System.getSecurityManager()
    val group = if (s != null) s.getThreadGroup() else Thread.currentThread().getThreadGroup()

    def newThread(r: Runnable): Thread = {
        val t = new Thread(group, r, namePrefix + "-thread-" + threadNumber.getAndIncrement(), 0)
        if (t.isDaemon())
            t.setDaemon(false)
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY)
        t
    }

}

object CryptHelper {

    val ALGORITHM__MD5 = "MD5";
    val ALGORITHM__SHA = "SHA";
    val ALGORITHM__HMAC_MD5 = "HmacMD5";
    val ALGORITHM__MD5withRSA = "MD5withRSA";
    val ALGORITHM__SHA1WithRSA = "SHA1WithRSA";

    val ALGORITHM__RSA = "RSA";
    val ALGORITHM__AES = "AES";
    val ALGORITHM__BLOWFISH = "Blowfish";
    val ALGORITHM__DES = "DES";
    val ALGORITHM__DESEDE = "DESede";

    def toHexString(in: Array[Byte]): String = {
        val len = in.length;
        val sb = new StringBuilder(len * 2);
        var i = 0
        while (i < len) {
            val tmp = Integer.toHexString(in(i) & 0xFF);
            if (tmp.length() < 2) {
                sb.append(0);
            }
            sb.append(tmp);
            i += 1
        }
        sb.toString();
    }

    def toBytes(hexString: String): Array[Byte] = {
        val len = hexString.length() / 2;
        val out = new Array[Byte](len);
        var pos = 0;
        var i = 0
        while (i < len) {
            out(i) = (Character.digit(hexString.charAt(pos), 16) << 4 | Character.digit(hexString.charAt(pos + 1), 16)).toByte
            i += 1
            pos += 2
        }
        return out;
    }

    def sign(source: Array[Byte], algorithm: String): Array[Byte] = {
        if (source != null) {
            try {
                val md5 = MessageDigest.getInstance(algorithm);
                return md5.digest(source)
            } catch {
                case e: Throwable =>
            }
        }
        return null
    }

    def md5(source: String, charset: String = "UTF-8"): String = {
        toHexString(sign(source.getBytes(charset), ALGORITHM__MD5))
    }

    def encryptHex(algorithm: String, hexKey: String, data: String, charset: String = "UTF-8"): String = {
        try {
            return toHexString(encrypt(algorithm, toBytes(hexKey), data.getBytes(charset)));
        } catch {
            case e: Throwable =>
                return null;
        }
    }

    def decryptHex(algorithm: String, hexKey: String, hexData: String, charset: String = "UTF-8"): String = {
        try {
            return new String(decrypt(algorithm, toBytes(hexKey), toBytes(hexData)), charset);
        } catch {
            case e: Throwable =>
                return null;
        }
    }

    def encrypt(algorithm: String, key: Array[Byte], data: Array[Byte]): Array[Byte] = {
        try {
            algorithm match {
                case ALGORITHM__DES =>
                    val desKeySpec = new DESKeySpec(key);
                    val keyFactory = SecretKeyFactory.getInstance(algorithm);
                    val desKey = keyFactory.generateSecret(desKeySpec);
                    return encrypt(algorithm, desKey, data);
                case _ =>
                    return null
            }
        } catch {
            case e: Throwable =>
                return null;
        }
    }

    def decrypt(algorithm: String, key: Array[Byte], data: Array[Byte]): Array[Byte] = {
        try {
            algorithm match {
                case ALGORITHM__DES =>
                    val desKeySpec = new DESKeySpec(key);
                    val keyFactory = SecretKeyFactory.getInstance(algorithm);
                    val desKey = keyFactory.generateSecret(desKeySpec);
                    return decrypt(algorithm, desKey, data);
                case _ =>
                    return null
            }
        } catch {
            case e: Throwable =>
                return null;
        }
    }

    def encrypt(algorithm: String, key: Key, data: Array[Byte]): Array[Byte] = {
        try {
            val cipher = Cipher.getInstance(algorithm);
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return cipher.doFinal(data);
        } catch {
            case e: Throwable =>
                return null;
        }
    }

    def decrypt(algorithm: String, key: Key, data: Array[Byte]): Array[Byte] = {
        try {
            val cipher = Cipher.getInstance(algorithm);
            cipher.init(Cipher.DECRYPT_MODE, key);
            return cipher.doFinal(data);
        } catch {
            case e: Throwable =>
                return null;
        }
    }

}

object LocalStorage {

    def save(key: String, value: String) {
        val dir = Router.dataDir + File.separator + "localstorage"
        val fdir = new File(dir)
        if (!fdir.exists()) fdir.mkdirs()
        val filename = dir + File.separator + key
        val f = new File(filename)
        FileUtils.writeStringToFile(f, if (value == null) "" else value, "UTF-8")
    }

    def save(key: String, o: HashMapStringAny) {
        save(key, JsonCodec.mkString(o))
    }

    def save(key: String, a: ArrayBufferString) {
        save(key, JsonCodec.mkString(a))
    }

    def save(key: String, a: ArrayBufferInt) {
        save(key, JsonCodec.mkString(a))
    }

    def save(key: String, a: ArrayBufferLong) {
        save(key, JsonCodec.mkString(a))
    }

    def save(key: String, a: ArrayBufferDouble) {
        save(key, JsonCodec.mkString(a))
    }

    def save(key: String, a: ArrayBufferMap) {
        save(key, JsonCodec.mkString(a))
    }

    def loadString(key: String): String = {
        val filename = Router.dataDir + File.separator + "localstorage" + File.separator + key
        val f = new File(filename)
        if (!f.exists()) return ""
        val s = FileUtils.readFileToString(f, "UTF-8")
        s
    }

    def loadMap(key: String): HashMapStringAny = {
        val s = loadString(key)
        val o = JsonCodec.parseObject(s)
        o
    }

    def loadStringArray(key: String): ArrayBufferString = {
        val s = loadString(key)
        val tt = JsonCodec.parseArray(s)
        if (tt == null) return null
        val a = new ArrayBufferString()
        for (t <- tt) a += TypeSafe.anyToString(t)
        a
    }

    def loadIntArray(key: String): ArrayBufferInt = {
        val s = loadString(key)
        val tt = JsonCodec.parseArray(s)
        if (tt == null) return null
        val a = new ArrayBufferInt()
        for (t <- tt) a += TypeSafe.anyToInt(t)
        a
    }

    def loadLongArray(key: String): ArrayBufferLong = {
        val s = loadString(key)
        val tt = JsonCodec.parseArray(s)
        if (tt == null) return null
        val a = new ArrayBufferLong()
        for (t <- tt) a += TypeSafe.anyToLong(t)
        a
    }
    
    def loadDoubleArray(key: String): ArrayBufferDouble = {
        val s = loadString(key)
        val tt = JsonCodec.parseArray(s)
        if (tt == null) return null
        val a = new ArrayBufferDouble()
        for (t <- tt) a += TypeSafe.anyToDouble(t)
        a
    }
    
    def loadMapArray(key: String): ArrayBufferMap = {
        val s = loadString(key)
        val tt = JsonCodec.parseArray(s)
        if (tt == null) return null
        val a = new ArrayBufferMap()
        for (t <- tt) a += t.asInstanceOf[HashMapStringAny]
        a
    }

}

