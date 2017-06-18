package scalabpe.core

import java.io.File
import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.LinkedHashMap
import scala.io.Source
import scala.tools.nsc.Global
import scala.tools.nsc.Settings
import scala.tools.nsc.reporters.ConsoleReporter

class FlowCompiler(val rootDir: String) extends Logging {

    val composeDir = rootDir + File.separator + "compose_conf"
    val libDir = rootDir + File.separator + "lib"
    val tempDir = Router.tempDir

    val flowTsFile = tempDir + File.separator + "flowts"
    val scalaflowTsFile = tempDir + File.separator + "scalaflowts"
    val scalaTsFile = tempDir + File.separator + "scalats"
    val libTsFile = tempDir + File.separator + "libts"

    val flowTsMap = new LinkedHashMap[String, Long]()
    val scalaflowTsMap = new LinkedHashMap[String, Long]()
    val scalaTsMap = new LinkedHashMap[String, Long]()
    val libTsMap = new LinkedHashMap[String, Long]()

    def error(message: String): Nothing = {
        throw new RuntimeException(message)
    }

    def initTsMap(f: String, map: LinkedHashMap[String, Long]): Unit = {

        if (!new File(f).exists) return

        val lines = Source.fromFile(f, "UTF-8").getLines.toList
        for (line <- lines) {
            val ss = line.split(":")
            map.put(ss(0), ss(1).toLong)
        }

    }

    def saveTsMap(f: String, map: LinkedHashMap[String, Long]): Unit = {

        val writer = new PrintWriter(new File(f), "UTF-8")
        for ((key, value) <- map) {
            writer.println(key + ":" + value)
        }
        writer.close()

    }

    def checkLibJarChanged(): Boolean = {

        val alllibfiles = listfiles(libDir, ".jar")
        val libfiles = new ArrayBufferString()
        var changed = false
        for (f <- alllibfiles) {
            val file = new File(f)
            val lmt = libTsMap.getOrElse(f, 0)
            if (lmt == 0 || lmt != file.lastModified) {
                libfiles += f
                libTsMap.put(f, file.lastModified)
                changed = true
            }

        }
        //println("changed="+changed)
        changed
    }

    def listfiles(dir: String, suffix: String, exclude: String = "_"): List[String] = {
        var dirs = ArrayBuffer[File](new File(dir))
        var newdirs = ArrayBuffer[File]()
        val files = ArrayBuffer[File]()

        while (dirs.size > 0) {
            for (d <- dirs) {
                val fs = d.listFiles()
                fs.filter(f => !f.isDirectory).foreach(files += _)
                fs.filter(f => f.isDirectory).foreach(newdirs += _)
            }
            dirs = newdirs
            newdirs = ArrayBuffer[File]()
        }
        val allfiles = files.map(_.getPath).filter(name => name.endsWith(suffix) && !name.endsWith(exclude)).toList
        allfiles
    }

    def checkScalaFilesChanged(): ArrayBufferString = {

        val allscalafiles = listfiles(composeDir, ".scala", ".flow.scala")

        val scalafiles = new ArrayBufferString()
        for (f <- allscalafiles) {
            val file = new File(f)
            val lmt = scalaTsMap.getOrElse(f, 0)
            if (lmt == 0 || lmt != file.lastModified) {
                scalafiles += f
                scalaTsMap.put(f, file.lastModified)
            }

        }

        scalafiles
    }

    def checkFlowFilesChanged(): ArrayBufferString = {

        val allflowfiles = listfiles(composeDir, ".flow")

        val flowfiles = new ArrayBufferString()
        for (f <- allflowfiles) {
            val file = new File(f)
            val lmt = flowTsMap.getOrElse(f, 0)
            if (lmt == 0 || lmt != file.lastModified) {
                flowfiles += f
                flowTsMap.put(f, file.lastModified)
            }

        }

        flowfiles
    }

    def checkScalaFlowFilesChanged(): ArrayBufferString = {

        val allscalaflowfiles = listfiles(composeDir, ".flow.scala")

        val flowfiles = new ArrayBufferString()
        for (f <- allscalaflowfiles) {
            val file = new File(f)
            val lmt = scalaflowTsMap.getOrElse(f, 0)
            if (lmt == 0 || lmt != file.lastModified) {
                flowfiles += f
                scalaflowTsMap.put(f, file.lastModified)
            }

        }

        flowfiles
    }

    def compile(): Boolean = {

        val t1 = System.currentTimeMillis

        val classesDir = tempDir + File.separator + "classes" + File.separator + "scalabpe" + File.separator + "flow"
        val srcDir = tempDir + File.separator + "src"
        val classesFile = new File(classesDir)
        classesFile.mkdirs()
        val srcFile = new File(srcDir)
        srcFile.mkdirs()

        new File(tempDir + File.separator + "compile").delete
        new File(tempDir + File.separator + "core").delete

        initTsMap(flowTsFile, flowTsMap)
        initTsMap(scalaflowTsFile, scalaflowTsMap)
        initTsMap(scalaTsFile, scalaTsMap)
        initTsMap(libTsFile, libTsMap)

        val libChanged = checkLibJarChanged()
        if (libChanged) { // need compile all files
            scalaTsMap.clear()
            flowTsMap.clear()
            scalaflowTsMap.clear()
            classesFile.listFiles.foreach(_.delete)
            srcFile.listFiles.foreach(_.delete)
        }

        var scalafiles = checkScalaFilesChanged()
        if (scalafiles.size > 0) { // need compile all files
            scalaTsMap.clear()
            flowTsMap.clear()
            scalaflowTsMap.clear()
            classesFile.listFiles.foreach(_.delete)
            srcFile.listFiles.foreach(_.delete)
            scalafiles = checkScalaFilesChanged() // reload all scala timestamp
        }
        val flowfiles = checkFlowFilesChanged
        val scalaflowfiles = checkScalaFlowFilesChanged

        if (libChanged || scalafiles.size > 0)
            log.info("compiling all ...")
        else if (flowfiles.size > 0 || scalaflowfiles.size > 0)
            log.info("compiling changed flows ...")
        else
            log.info("no files need to be compiled ...")

        val settings = new Settings(error)

        var cp = new File(libDir).listFiles().filter(f => f.isDirectory).map(f => libDir + File.separator + f.getName + File.separator + "*").mkString(File.pathSeparator)
        cp = libDir + File.separator + "*" + File.pathSeparator + cp
        cp = cp + File.pathSeparator + tempDir + File.separator + "classes" + File.pathSeparator + "."
        settings.classpath.value = cp

        settings.outdir.value = new File(tempDir + File.separator + "classes").getPath
        settings.deprecation.value = false
        settings.unchecked.value = true
        settings.encoding.value = "UTF-8"

        val reporter = new ConsoleReporter(settings)

        val compiler = new Global(settings, reporter)

        if (scalafiles.size > 0) {
            (new compiler.Run).compile(scalafiles.toList)
            reporter.printSummary
            if (reporter.hasErrors || reporter.WARNING.count > 0) {
                return false
            }
        }

        if (scalaflowfiles.size > 0) {
            (new compiler.Run).compile(scalaflowfiles.toList)
            reporter.printSummary
            if (reporter.hasErrors || reporter.WARNING.count > 0) {
                return false
            }
        }

        if (flowfiles.size > 0) {
            val tmpsrcfiles = flowfiles.map(generateTempSrcFile)
            (new compiler.Run).compile(tmpsrcfiles.toList)
            reporter.printSummary
            if (reporter.hasErrors || reporter.WARNING.count > 0) {
                return false
            }
        }

        if (flowfiles.size > 0) {
            saveTsMap(flowTsFile, flowTsMap)
        }
        if (scalaflowfiles.size > 0) {
            saveTsMap(scalaflowTsFile, scalaflowTsMap)
        }
        if (scalafiles.size > 0) {
            saveTsMap(scalaTsFile, scalaTsMap)
        }
        if (libChanged) {
            saveTsMap(libTsFile, libTsMap)
        }

        val t2 = System.currentTimeMillis
        log.info("compiling finished, ts=%s[ms]".format(t2 - t1))
        true
    }

    val package_template = "package scalabpe.flow;import scalabpe.core._;"
    val class2_template = "class Flow_%s extends %s { "

    val flow_class = "Flow"
    val syncedflow_class = "SyncedFlow"

    val function_template = "}; def %s() {"
    val function_template_2 = "override def receive() {"
    val tail_template = "}}"
    val tail_template_2 = "}"

    val tag_class = "//$"
    val tag_def = "//#"
    val tag_receive = "receive"
    val tag_synced = "withsyncedinvoke"
    val tag_with = "with("

    def generateTempSrcFile(filename: String): String = {

        val filepath = filename.substring(2).replace("/", "_").replace("\\", "_")
        val tmpfile = tempDir + File.separator + "src" + File.separator + filepath.replace(".flow", ".scala")
        val lines = Source.fromFile(filename, "UTF-8").getLines.toList
        val writer = new PrintWriter(new File(tmpfile), "UTF-8")
        writer.print(package_template) // not println!
        
        var hasFunction = false
        for (line <- lines) {
            if (line.startsWith(tag_class)) {
                val s = line.substring(3).replace(".", "###").toLowerCase
                val ss = s.split("###")
                val classname = ss(0) + "_" + ss(1)
                val baseclassname = parseBaseClass(line)
                val newline = class2_template.format(classname, baseclassname)
                writer.println(newline)
            } else if (line.startsWith(tag_def)) {
                val funname = line.substring(3)
                val newline = if (funname == tag_receive) function_template_2 else function_template.format(funname)
                writer.println(newline)
                hasFunction = true
            } else {
                writer.println(line)
            }
        }
        
        if(hasFunction)
            writer.println(tail_template)
        else    
            writer.println(tail_template_2)
            
        writer.close()
        tmpfile
    }

    def parseBaseClass(line: String): String = {
        val lines = line.substring(3)
        val p1 = lines.indexOf(".")
        if (p1 < 0) return flow_class
        val p2 = lines.indexOf(".", p1 + 1)
        if (p2 < 0) return flow_class
        val s = lines.substring(p2 + 1)
        if (s == null || s == "") return flow_class
        if (s.toLowerCase == tag_synced) return syncedflow_class
        if (s.startsWith(tag_with) && s.endsWith(")")) {
            val t = s.substring(5, s.length - 1)
            if (t.indexOf(".") >= 0) return t
            else return "scalabpe.flow." + t
        }
        flow_class
    }

    def genScalaFilesOnly(): Boolean = {

        val t1 = System.currentTimeMillis

        val srcDir = tempDir + File.separator + "src"
        val srcFile = new File(srcDir)
        srcFile.mkdirs()

        initTsMap(flowTsFile, flowTsMap)
        initTsMap(scalaflowTsFile, scalaflowTsMap)
        initTsMap(scalaTsFile, scalaTsMap)

        var scalafiles = checkScalaFilesChanged()
        if (scalafiles.size > 0) { // need compile all files
            scalaTsMap.clear()
            flowTsMap.clear()
            srcFile.listFiles.foreach(_.delete)
            scalafiles = checkScalaFilesChanged() // reload all scala timestamp
        }
        val flowfiles = checkFlowFilesChanged
        val scalaflowfiles = checkScalaFlowFilesChanged

        if (scalafiles.size > 0)
            log.info("generating all ...")
        else if (flowfiles.size > 0 || scalaflowfiles.size > 0)
            log.info("generating changed flows ...")
        else
            log.info("no files need to be generating ...")

        if (flowfiles.size > 0) {
            val tmpsrcfiles = flowfiles.map(generateTempSrcFile)
        }

        if (flowfiles.size > 0) {
            saveTsMap(flowTsFile, flowTsMap)
        }
        if (scalaflowfiles.size > 0) {
            saveTsMap(scalaflowTsFile, scalaflowTsMap)
        }
        if (scalafiles.size > 0) {
            saveTsMap(scalaTsFile, scalaTsMap)
        }
        val t2 = System.currentTimeMillis
        log.info("generating finished, ts=%s[ms]".format(t2 - t1))
        true
    }

}

object ScalaFileGenerator {

    def main(args: Array[String]) {
        val c = new FlowCompiler(".")
        c.genScalaFilesOnly()
    }

}

