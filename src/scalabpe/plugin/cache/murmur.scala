package scalabpe.plugin.cache

import java.nio.ByteBuffer
import java.nio.ByteOrder;

object MurmurHash {

    def hash(key: String): Long = {
        return hash(key.getBytes("utf-8"));
    }

    def hash(key: Array[Byte]): Long = {
        return hash64A(key, 305441741);
    }

    def hash64A(data: Array[Byte], seed: Int): Long = {
        return hash64A(ByteBuffer.wrap(data), seed);
    }

    def hash64A(buf: ByteBuffer, seed: Int): Long = {
        val byteOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);

        val m = -4132994306676758123L;
        val r = 47;

        var h = seed ^ buf.remaining() * m;

        while (buf.remaining() >= 8) {
            var k = buf.getLong();

            k = k * m;
            k = k ^ (k >>> r);
            k = k * m;

            h ^= k;
            h *= m;
        }

        if (buf.remaining() > 0) {
            val finish = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);

            finish.put(buf).rewind();
            h ^= finish.getLong();
            h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        buf.order(byteOrder);
        return h;
    }

}

