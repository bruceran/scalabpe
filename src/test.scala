package jvmdbbroker.core

import java.util.concurrent.atomic.{AtomicBoolean,AtomicInteger}
import java.io._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.xml._

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

class TestCase(val serviceId:Int,val msgId:Int,val body:HashMapStringAny,val repeat:Int = 1)

object TestCaseRunner {

    var sendCount = 0
    var receiveCount = 0
    var soc : SocImpl = _

    val shutdown = new AtomicBoolean()
    def callback(any:Any){

        any match {

            case reqRes: SocRequestResponseInfo =>

                val reqRes = any.asInstanceOf[SocRequestResponseInfo]

                println("res="+reqRes.res)

                receiveCount += 1
                if( receiveCount == sendCount ) {
                    shutdown.set(true)
                }

            case ackInfo: SocRequestAckInfo =>

                println("ack="+ackInfo.req.requestId)

            case _ =>
        }
    }

    def main(args:Array[String]) {

        var file = "."+File.separator+"testcase"+File.separator+"default.txt"
        if( args.size >= 1 ) file = args(0)
        println("running file:  " + file)

        val codecs = new TlvCodecs("."+File.separator+"avenue_conf")

        val in = new InputStreamReader(new FileInputStream("."+File.separator+"config.xml"),"UTF-8")
        val cfgXml = XML.load(in)
        val port = (cfgXml \ "SapPort").text.toInt
        val timeout = 15000
        var serverAddr = "127.0.0.1:"+port
        var s = (cfgXml \ "TestServerAddr").text
        if( s != "" ) serverAddr = s

        soc = new SocImpl(serverAddr,codecs,callback,connSizePerAddr=1)

        val lines = Source.fromFile(file,"UTF-8").getLines.toList.filter( _.trim != "").filter( !_.startsWith("#") )

        val testcases = parseLines(lines)

        sendCount = testcases.foldLeft(0) { (sum,tc) => sum + tc.repeat }

        println("total request count : "+sendCount)

        var seq = 0

        for( i <- 1 to testcases.size ) {

            val t = testcases(i-1)

            for( j <- 1 to t.repeat ) {

                seq += 1
                val req = new SocRequest(seq.toString,t.serviceId,t.msgId,t.body)
                println("req="+req)
                soc.send(req,timeout)
            }

        }

        while( !shutdown.get ) {
            Thread.sleep(500)
        }

        soc.close
    }

    def parseLines(lines: List[String]) : ArrayBuffer[TestCase] = {

        val testcases = new ArrayBuffer[TestCase]()

        for( line <- lines ) {
            val tc = parseLine(line)
            if( tc != null )
                testcases += tc
        }

        testcases
    }

    def parseLine(line:String) : TestCase = {

        val p1 = line.indexOf(",")
        if( p1 <= 0 ) return null

        val p2 = line.indexOf(",",p1+1)
        if( p2 <= 0 ) return null

        val serviceId = line.substring(0,p1).toInt
        val msgId = line.substring(p1+1,p2).toInt
        val json = line.substring(p2+1)

        val body = JsonCodec.parseObject(json)

        var repeat = body.i("x_repeat")
        if( repeat == 0 ) repeat = 1

        val tc = new TestCase(serviceId,msgId,body,repeat)
        tc
    }

}

