package scalabpe.plugin.http

import java.io._

import org.jboss.netty.buffer._
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.handler.codec.frame.TooLongFrameException
import org.jboss.netty.handler.codec.http.HttpChunk
import org.jboss.netty.handler.codec.http.HttpChunkTrailer
import org.jboss.netty.handler.codec.http.HttpHeaders
import org.jboss.netty.handler.codec.http.HttpMessage

import scalabpe.core._

class HttpFileUploadAggregator2(val dir: String, val maxContentLength: Int = 65535)
        extends SimpleChannelUpstreamHandler with Logging {

    val CR = '\r'.toByte
    val NL = '\n'.toByte
    val charset = "ISO-8859-1"

    var tempUploaded: Int = 0
    var tempMessage: HttpMessage = null
    var lastContent = ChannelBuffers.dynamicBuffer(50000)
    var data = ArrayBufferMap() 
    var boundary :Array[Byte] = null
    var tempFile: String = null
    var tempFileOutputStream: OutputStream = null

    def uuid(): String = {
        return java.util.UUID.randomUUID().toString().replaceAll("-", "")
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {

        val currentMessage = tempMessage
        val msg = e.getMessage()

        msg match {
            case m: HttpMessage =>
                val contentType = m.getHeader(HttpHeaders.Names.CONTENT_TYPE)
                if (m.isChunked() && contentType.indexOf("multipart/form-data") >= 0) {

                    val encodings = m.getHeaders(HttpHeaders.Names.TRANSFER_ENCODING)
                    encodings.remove(HttpHeaders.Values.CHUNKED)
                    if (encodings.isEmpty()) {
                        m.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING)
                    }

                    m.setChunked(false)
                    m.setContent(ChannelBuffers.dynamicBuffer(e.getChannel().getConfig().getBufferFactory()))
                    this.tempMessage = m
                } else {
                    this.tempMessage = null
                    ctx.sendUpstream(e)
                }

            case chunk: HttpChunk =>
                if (currentMessage == null) {
                    ctx.sendUpstream(e)
                    return
                }

                val content = chunk.getContent()
                val len = content.readableBytes()

                if (tempUploaded + len > maxContentLength) {
                    closeTempFile()
                    reset()
                    // todo delete temp file
                    throw new TooLongFrameException("HTTP content length exceeded " + maxContentLength + " bytes.")
                }

                tempUploaded += len

                if( tempFile == null ) {
                    lastContent.writeBytes(content)
                }  else {
                    if( lastContent.readableBytes == 0 ) { // 减少内存拷贝
                    
                        val bak = lastContent
                        lastContent = content
                        val finished = readMultiPartFile()
                        lastContent = bak
                        if( content.readableBytes > 0 ) {
                            lastContent.writeBytes(content)
                        }

                        if( !finished ) {
                            return
                        } 
                    } else {
                        lastContent.writeBytes(content)

                        val finished = readMultiPartFile()
                        if( !finished ) {
                            lastContent.discardReadBytes()
                            return
                        }
                    }
                }

                if( boundary == null ) {
                    boundary = parseBoundary()
                    if( boundary == null ) {
                        if( lastContent.readableBytes >= 200 ) {
                            throw new TooLongFrameException("HTTP boundary not found in first 200 bytes")
                        }
                        return
                    }
                }

                while( readNextPart() ) {}

                lastContent.discardReadBytes()

                if (chunk.isLast()) {

                    if (chunk.isInstanceOf[HttpChunkTrailer]) {
                        val trailer = chunk.asInstanceOf[HttpChunkTrailer]
                        val i = trailer.getHeaders().iterator()
                        while (i.hasNext()) {
                            val header = i.next()
                            currentMessage.setHeader(header.getKey(), header.getValue())
                        }
                    }

                    currentMessage.setHeader(HttpHeaders.Names.CONTENT_LENGTH, tempUploaded)
                    val json_data = JsonCodec.mkString(data)
                    currentMessage.setHeader("X_UPLOAD_PROCESSED_2", json_data )

                    reset()

                    Channels.fireMessageReceived(ctx, currentMessage, e.getRemoteAddress())
                }
            case _ =>
                ctx.sendUpstream(e)
        }
    }

    def reset() {
        tempFile = null
        tempFileOutputStream = null
        tempUploaded = 0
        tempMessage = null
        lastContent.clear()
        boundary = null
        data.clear()
    }

    def closeTempFile() {
        try {
            if (tempFileOutputStream != null)
                tempFileOutputStream.close()
        } catch {
            case e: Throwable =>
        }
    }

    def readNextPart():Boolean = {
        var saved = lastContent.readerIndex
        val m = parsePartAttrs()
        if( m == null ) { 
            lastContent.readerIndex(saved)
            return false
        }

        if (m.contains("filename")) {
            val existed = new File(dir).exists()
            if (!existed) {
                new File(dir).mkdirs()
            }

            tempFile = dir + "/" + uuid() + ".tmp"
            tempFileOutputStream = new BufferedOutputStream(new FileOutputStream(tempFile), 5000000);

            m.put("file", tempFile)
            data += m 
            return readMultiPartFile()
        } else {
            val v = readMultiPartValue()
            if( v == null ) { 
                lastContent.readerIndex(saved)
                return false
            }
            m.put("value", v)
            data += m 
            return true
        }
    }

    def readMultiPartFile():Boolean = {
        while(true) {
            var i = findCr()
            if ( i == -1 ) {
                val len = lastContent.readableBytes
                lastContent.readBytes(tempFileOutputStream, len)
                lastContent.clear()
                return false
            }

            val (matched, nextp) = compareBoundary(i)
            val len = i
            if( len > 0 )
                lastContent.readBytes(tempFileOutputStream, len)

            if (matched) {
                closeTempFile()
                tempFile = null
                tempFileOutputStream = null
                lastContent.readerIndex(nextp)
                return true
            } else {
                if( nextp == lastContent.writerIndex ) { // partial match, need next chunk to compare boundary
                    return false
                } else { // ignore current cr, continue findcr
                    lastContent.readBytes(tempFileOutputStream, 1)
                }
            }
        }
        return false
    }

    def readMultiPartValue(): String = {
        val p1 = lastContent.readerIndex
        val p2 = lastContent.writerIndex
        var i = p1
        while( i < p2 ) {
            val b = lastContent.getByte(i)
            if (b == CR ) {
                val (matched, nextp) = compareBoundary(i-p1)
                if (matched) {
                    val bs = new Array[Byte](i-p1)
                    lastContent.readBytes(bs)
                    val v = new String(bs,charset)
                    lastContent.readerIndex(nextp)
                    return v
                } 
            }
            i += 1
        }

        return null
    }

    // 返回: Tuple2[_1,_2]  _1 是否匹配  _2 匹配的最后一个字符的下一个位置
    def compareBoundary(si:Int): Tuple2[Boolean, Int] = {
        val p1 = lastContent.readerIndex
        val p2 = lastContent.writerIndex
        var i = p1 + si

        // match \n
        i += 1 
        if( i >= p2 ) return Tuple2(false,i)
        val b = lastContent.getByte(i)
        if (b != NL ) {
            return Tuple2(false,i)
        } 

        // match boundary
        i += 1 
        if( i >= p2 ) return Tuple2(false,i)

        var j = 0
        while (j < boundary.size && i < p2 ) {
            val b = lastContent.getByte(i)
            if (b != boundary(j)) return new Tuple2(false, i)
            j += 1
            i += 1
        }

        if( i >= p2 ) return new Tuple2(true,i)
        if( i+1 >= p2 ) return new Tuple2(true,i)

        val b1 = lastContent.getByte(i)
        val b2 = lastContent.getByte(i+1)

        if(  (b1 == '-' && b2 == '-') || (b1 == CR && b2 == NL ) ) {
            i += 2
        }
        (true, i)
    }

    def parsePartAttrs(): HashMapStringAny = {

        val str = parsePart()
        if (str == null)  return null
        val map = HashMapStringAny()
        val lines = str.split("\r\n")
        for (line <- lines) {
            val p = line.indexOf(":")
            if (p > 0) {
                val key = line.substring(0, p)
                key match {
                    case "Content-Disposition" =>
                        val s = line.substring(p + 1).trim
                        val ss = s.split(";")
                        for (ts <- ss) {
                            val tss = ts.trim.split("=")
                            tss(0) match {
                                case "name" =>
                                    val v = tss(1).replace("\"", "")
                                    map.put("name", v)
                                case "filename" =>
                                    var v = tss(1).replace("\"", "")
                                    val p1 = v.lastIndexOf("/")
                                    if (p1 >= 0) v = v.substring(p1 + 1)
                                    val p2 = v.lastIndexOf("\\")
                                    if (p2 >= 0) v = v.substring(p2 + 1)
                                    map.put("filename", v)
                                case _ =>
                            }
                        }
                    case "Content-Type" =>
                        val contentType = line.substring(p + 1).trim()
                        map.put("contentType", contentType)
                    case _ =>
                }
            }
        }
        map
    }

    def parsePart(): String = {
        val p1 = lastContent.readerIndex
        val p2 = lastContent.writerIndex
        var i = p1
        while( i < p2 ) {
            val b = lastContent.getByte(i)
            if (b == '\n') {
                val bs = new Array[Byte](i-p1+1)
                lastContent.getBytes(p1,bs)
                val line = new String(bs,charset)
                if (line.endsWith("\r\n\r\n")) {
                    lastContent.readerIndex(i+1)
                    return line
                }
            }
            i += 1
        }
        return null
    }

    def parseBoundary():Array[Byte] = {
        val i = findNl() 
        if (i == -1 ) return null
        val bs = new Array[Byte](i+1)
        val p1 = lastContent.readerIndex
        lastContent.getBytes(p1,bs)

        val s = new String(bs).trim()
        val bs2 = s.getBytes()
        lastContent.readerIndex(p1+i+1)
        return bs2
    }

    def findCr(): Int = {
        val p1 = lastContent.readerIndex
        val p2 = lastContent.writerIndex
        val i = lastContent.indexOf(p1,p2,CR) 
        if( i == -1 ) return -1
        return i - p1
    }

    def findNl(): Int = {
        val p1 = lastContent.readerIndex
        val p2 = lastContent.writerIndex
        val i = lastContent.indexOf(p1,p2,NL) 
        if( i == -1 ) return -1
        return i - p1
    }

}

