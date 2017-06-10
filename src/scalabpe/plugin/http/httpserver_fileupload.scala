package scalabpe.plugin.http

import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.OutputStream

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.handler.codec.frame.TooLongFrameException
import org.jboss.netty.handler.codec.http.HttpChunk
import org.jboss.netty.handler.codec.http.HttpChunkTrailer
import org.jboss.netty.handler.codec.http.HttpHeaders
import org.jboss.netty.handler.codec.http.HttpMessage

import scalabpe.core.Logging

class HttpFileUploadAggregator(val dir: String, val maxContentLength: Int = 65535)
        extends SimpleChannelUpstreamHandler with Logging {

    var tempFile: String = null
    var tempFileOutputStream: OutputStream = null
    var tempUploaded: Int = 0
    var tempMessage: HttpMessage = null

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

                    tempFile = dir + "/" + uuid() + ".uptmp"
                    tempFileOutputStream = new BufferedOutputStream(new FileOutputStream(tempFile))

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

                if (tempUploaded + chunk.getContent().readableBytes() > maxContentLength) {
                    closeFile()
                    reset()
                    throw new TooLongFrameException("HTTP content length exceeded " + maxContentLength + " bytes.")
                }

                val content = chunk.getContent()
                val len = content.readableBytes()
                tempUploaded += len
                content.readBytes(tempFileOutputStream, len)
                if (chunk.isLast()) {
                    closeFile()

                    if (chunk.isInstanceOf[HttpChunkTrailer]) {
                        val trailer = chunk.asInstanceOf[HttpChunkTrailer]
                        val i = trailer.getHeaders().iterator()
                        while (i.hasNext()) {
                            val header = i.next()
                            currentMessage.setHeader(header.getKey(), header.getValue())
                        }
                    }

                    currentMessage.setHeader(HttpHeaders.Names.CONTENT_LENGTH, tempUploaded)
                    currentMessage.setHeader("X_UPLOAD_PROCESSED", this.tempFile)

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
    }

    def closeFile() {
        try {
            if (tempFileOutputStream != null)
                tempFileOutputStream.close()
        } catch {
            case e: Throwable =>
        }
    }
}
