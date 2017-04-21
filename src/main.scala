package scalabpe.core

import java.io._
import sun.misc.Signal;
import sun.misc.SignalHandler;
import scala.xml._

object Main extends Logging with SignalHandler {

    var router : Router = _
    var mainThread : Thread = _
    var shutdown = false
    var selfCheckServer : SelfCheckServer = _
    var testMode = false
    var stopFile = ""

    def handle(signal:Signal) {
        if(shutdown) return
        println("# signalHandler: " + signal.getName())
        mainThread.interrupt()
    }

    def initProfile(rootDir:String) {
        val profile = System.getProperty("scalabpe.profile")
        if( profile != null && profile != "") {
           Router.profile = profile
           Router.parameterXml = "parameter_"+profile+".xml"
           val filename = "config_"+profile+".xml"
           if( new File(rootDir+File.separator+filename).exists ) {
               Router.configXml = filename
           }
        }
        log.info("current profile="+Router.profile)
        log.info("use config file="+Router.configXml)
        log.info("use config paramter file="+Router.parameterXml)
    }

    def main(args:Array[String]) {

        System.setProperty("org.terracotta.quartz.skipUpdateCheck","true");

        mainThread = Thread.currentThread

        Signal.handle(new Signal("TERM"), this);
        Signal.handle(new Signal("INT"), this);

        val rootDir = "."

        stopFile = rootDir+File.separator+"cmd_stop"

        val t1 = System.currentTimeMillis

        initProfile(rootDir)

        val in = new InputStreamReader(new FileInputStream(rootDir+File.separator+Router.configXml),"UTF-8")
        val cfgXml = XML.load(in)
        in.close()

        Router.tempDir = rootDir+File.separator+"temp"
        val appName = System.getProperty("application.name")
        val tempDirRoot = System.getProperty("scalabpe.tempdirroot")
        if( tempDirRoot != null && tempDirRoot != "" ) Router.tempDir = tempDirRoot+File.separator+appName

        val t = System.getenv("runninginide")
        if( t != null && t == "yes" ) {
            log.info("running in ide, skip compiling")
        } else {
            val compiler = new FlowCompiler(rootDir)
            val ok = compiler.compile()
            if(!ok) {
                log.error("compile failed")
                return;
            }
        }

        val startSos = !testMode 
        val installMock = (cfgXml \ "InstallMock").text
        router = new Router(rootDir,startSos,testMode||(installMock!=""))

        val selfCheckPort = (cfgXml \ "CohPort").text.toInt

        if( selfCheckPort > 0 && startSos ) {
            selfCheckServer = new SelfCheckServer(selfCheckPort,router)
        }

        val t2 = System.currentTimeMillis
        log.info("scalabpe started, ts=%s[ms]".format(t2-t1))

        if( installMock != "" && !testMode ) {
            TestCaseRunner.installMock(installMock)
        }

        if( testMode ) {
            return
        }

        new File(stopFile).delete()

        while( !shutdown ) {
            try {
                Thread.sleep(1000)

                val f = new File(stopFile)
                if( f.exists ) {
                    f.delete()
                    shutdown = true
                }

            } catch {
                case e:Exception =>
                    shutdown = true
            }
        }

        close()
    }

    def close() {
    
        if( selfCheckServer != null )
            selfCheckServer.close()

        router.close()

        log.asInstanceOf[ch.qos.logback.classic.Logger].getLoggerContext().stop
    }
}

