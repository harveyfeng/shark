/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark

import java.io.BufferedReader
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.PrintStream
import java.io.UnsupportedEncodingException
import java.net.URLClassLoader
import java.util.ArrayList
import jline.{History, ConsoleReader}
import scala.collection.JavaConversions._

import org.apache.commons.lang.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.cli.{CliDriver, CliSessionState, OptionsProcessor}
import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.exec.{FunctionRegistry, Utilities}
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.parse.ParseDriver
import org.apache.hadoop.hive.ql.processors.{CommandProcessor, CommandProcessorFactory}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.io.IOUtils

import shark.streaming.StreamingDriver

import org.apache.spark.SparkContext


object SharkCliDriver {

  var prompt  = "streaming"
  var prompt2 = "         " // when ';' is not yet seen.
  val streamingPrompt  = "streaming"
  val streamingPrompt2 = "         "

  def main(args: Array[String]) {
    val hiveArgs = args.filterNot(_.equals("-loadRdds"))
    val loadRdds = hiveArgs.length < args.length
    val oproc = new OptionsProcessor()
    if (!oproc.process_stage1(hiveArgs)) {
      System.exit(1)
    }

    // NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    var logInitFailed = false
    var logInitDetailMessage: String = null
    try {
      logInitDetailMessage = LogUtils.initHiveLog4j()
    } catch {
      case e: LogInitializationException =>
        logInitFailed = true
        logInitDetailMessage = e.getMessage()
    }

    var ss = new CliSessionState(new HiveConf(classOf[SessionState]))
    ss.in = System.in
    try {
      ss.out = new PrintStream(System.out, true, "UTF-8")
      ss.info = new PrintStream(System.err, true, "UTF-8");
      ss.err = new PrintStream(System.err, true, "UTF-8")
    } catch {
      case e: UnsupportedEncodingException => System.exit(3)
    }

    if (!oproc.process_stage2(ss)) {
      System.exit(2)
    }

    if (!ss.getIsSilent()) {
      if (logInitFailed) System.err.println(logInitDetailMessage)
      else SessionState.getConsole().printInfo(logInitDetailMessage)
    }

    // Set all properties specified via command line.
    val conf: HiveConf = ss.getConf()
    ss.cmdProperties.entrySet().foreach { item: java.util.Map.Entry[Object, Object] =>
      conf.set(item.getKey().asInstanceOf[String], item.getValue().asInstanceOf[String])
      ss.getOverriddenConfigurations().put(
        item.getKey().asInstanceOf[String], item.getValue().asInstanceOf[String])
    }

    SessionState.start(ss)

    // Clean up after we exit
    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        override def run() {
          SharkEnv.stop()
        }
      }
    )

    // "-h" option has been passed, so connect to Shark Server.
    if (ss.getHost() != null) {
      ss.connect()
      if (ss.isRemoteMode()) {
        prompt = "[" + ss.getHost + ':' + ss.getPort + "] " + prompt
        val spaces = Array.tabulate(prompt.length)(_ => ' ')
        prompt2 = new String(spaces)
      }
    }

    if (!ss.isRemoteMode() && !ShimLoader.getHadoopShims().usesJobShell()) {
      // Hadoop-20 and above - we need to augment classpath using hiveconf
      // components.
      // See also: code in ExecDriver.java
      var loader = conf.getClassLoader()
      val auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS)
      if (StringUtils.isNotBlank(auxJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","))
      }
      conf.setClassLoader(loader)
      Thread.currentThread().setContextClassLoader(loader)
    }

    var cli = new SharkCliDriver(loadRdds)
    cli.setHiveVariables(oproc.getHiveVariables())

    // Execute -i init files (always in silent mode)
    cli.processInitFiles(ss)

    if (ss.execString != null) {
      System.exit(cli.processLine(ss.execString))
    }

    try {
      if (ss.fileName != null) {
        System.exit(cli.processFile(ss.fileName))
      }
    } catch {
      case e: FileNotFoundException =>
        System.err.println("Could not open input file for reading. (" + e.getMessage() + ")")
        System.exit(3)
    }

    var reader = new ConsoleReader()
    reader.setBellEnabled(false)
    // reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)))
    reader.addCompletor(CliDriver.getCommandCompletor())

    var line: String = null
    val HISTORYFILE = ".hivehistory"
    val historyDirectory = System.getProperty("user.home")
    try {
      if ((new File(historyDirectory)).exists()) {
        val historyFile = historyDirectory + File.separator + HISTORYFILE
        reader.setHistory(new History(new File(historyFile)))
      } else {
        System.err.println("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.")
      }
    } catch {
      case e: Exception =>
        System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
                           "history file.  History will not be available during this session.")
        System.err.println(e.getMessage())
    }

    // Use reflection to get access to the two fields.
    val getFormattedDbMethod = classOf[CliDriver].getDeclaredMethod(
      "getFormattedDb", classOf[HiveConf], classOf[CliSessionState])
    getFormattedDbMethod.setAccessible(true)

    val spacesForStringMethod = classOf[CliDriver].getDeclaredMethod(
      "spacesForString", classOf[String])
    spacesForStringMethod.setAccessible(true)

    var ret = 0

    var prefix = ""
    var curDB = getFormattedDbMethod.invoke(null, conf, ss).asInstanceOf[String]
    var curPrompt = SharkCliDriver.prompt + curDB
    var dbSpaces = spacesForStringMethod.invoke(null, curDB).asInstanceOf[String]
    var sharkMode = SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE)

    line = reader.readLine(curPrompt + "> ")

    // =======================================
    // for debugging
    val twitter = "drop table if exists src_stream;" +
      "drop table if exists src2_stream;" +
      "drop table if exists src_archive;" +
      "create stream src_stream(user string, lang string, country string, text string, retweets int, computeTime bigint) " +
        "as read directory '/Users/harveyfeng/testing/test/user-lang-country-text-retweets.txt' batch '4 seconds' ;" +
      "create table src_archive(user string, lang string, country string, text string, retweets int, computeTime bigint);" +
      "insert into table src_archive select * from src_stream window_4 batch '4 seconds';" +
      "start;"

    val lines2 = "drop table if exists src_stream;" +
      "drop table if exists src2_stream;" +
      "drop table if exists src_archive;" +
      "create stream src_stream(key int, value string, time bigint) " +
        "as read directory '/Users/harveyfeng/testing/test/kv1.txt' batch '4 seconds';" +
      "create stream src2_stream as select * from last '8 seconds' of src_stream batch '4 seconds';" +
      "create table src_archive(key int, value string, time bigint);" +
      "insert into table src_archive select * from last '8 seconds' of src2_stream batch '4 seconds';" +
      "start;"

    val lines3 = "drop table if exists src_stream;" +
      "drop table if exists src2_stream;" +
      "drop table if exists src_archive;" +
      "drop table if exists src_historical;" +
      "create table src_historical(key int, value string); " +
      "load data local inpath '/Users/harveyfeng/hive09/data/files/kv1.txt' into table src_historical; " +
      "create stream src_stream(key int, value string, time bigint) as read directory '/Users/harveyfeng/testing/test/kv1.txt' batch '4 seconds';" +
      "create stream src2_stream as select sh.key k, sh.value v, ss.key k2, ss.value, ss.time v2 from last '4 seconds' of src_stream ss join src_historical sh on sh.key=ss.key batch '4 seconds';" +
      "create table src_archive(key int, value string, key2 int, value2 string, time bigint);" +
      "insert into table src_archive select * from last '4 seconds' of src2_stream batch '4 seconds';" +
      "start;"

    // =======================================

    while (line != null) {
      if (line.contains("ttest")) {
        for (linecmd <- twitter.split(";")) {
          ret = cli.processLine(linecmd)
        }
        line = reader.readLine(curPrompt + "> ")
      }
      if (line.contains("restart")) {
        SharkEnv.streams.getSscs.foreach(_.start())
        line = reader.readLine(curPrompt + "> ")
      }
      if (line.contains("test3")) {
        for (linecmd <- lines3.split(";")) {
          ret = cli.processLine(linecmd)
        }
        line = reader.readLine(curPrompt + "> ")
      }
      if (line.contains("test2")) {
        for (linecmd <- lines2.split(";")) {
          ret = cli.processLine(linecmd)
        }
        line = reader.readLine(curPrompt + "> ")
      }
      if (line.contains("check streams")) {
        val streamManager = SharkEnv.streams
        val memoryMetadataManager = SharkEnv.memoryMetadataManager
        line = reader.readLine(curPrompt + "> ")
      }

      if (!prefix.equals("")) {
        prefix += '\n'
      }
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line
        ret = cli.processLine(line)
        prefix = ""
        sharkMode = SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE)
        curPrompt = sharkMode match {
          case "shark" => SharkCliDriver.prompt
          case "streaming" => SharkCliDriver.streamingPrompt
          case _ => CliDriver.prompt
        }
      } else {
        prefix = prefix + line
        curPrompt = sharkMode match {
          case "shark" => SharkCliDriver.prompt2
          case "streaming" => SharkCliDriver.streamingPrompt2
          case _ => CliDriver.prompt2
        }
        curPrompt += dbSpaces
      }

      line = reader.readLine(curPrompt + "> ")
    }

    ss.close()

    System.exit(ret)
  }
}


class SharkCliDriver(loadRdds: Boolean = false) extends CliDriver with LogHelper {

  private val ss = SessionState.get()

  private val LOG = LogFactory.getLog("CliDriver")

  private val console = new SessionState.LogHelper(LOG)

  private val conf: Configuration = if (ss != null) ss.getConf() else new Configuration()

  SharkConfVars.initializeWithDefaults(conf);

  // Force initializing SharkEnv. This is put here but not object SharkCliDriver
  // because the Hive unit tests do not go through the main() code path.
  SharkEnv.init()

  if(loadRdds) CachedTableRecovery.loadAsRdds(processCmd(_))

  def this() = this(false)

  override def processCmd(cmd: String): Int = {
    val ss: SessionState = SessionState.get()
    val cmd_trimmed: String = cmd.trim()
    val tokens: Array[String] = cmd_trimmed.split("\\s+")
    val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
    var ret = 0

    /*
    if (cmd_trimmed.toLowerCase.equals("start")) {
      SharkEnv.streams.getSscs.foreach(_.start())
      val out = ss.out
      out.println("=============")
      out.println("Starting SSC")
      out.println("=============")
      return 0
    } else if (cmd_trimmed.toLowerCase.equals("stop")) {
      SharkEnv.streams.getSscs.foreach(_.stop())
      val out = ss.out
      out.println("=============")
      out.println("Stopping SSC")
      out.println("=============")
      return 0
    }
     */
    // Note vvv that might have to go in StreamingSemanticAnalyzer
    /*
    // If the StreamingContext used for this executor DStream hasn't been started, add a
    // StreamingLaunchTask as a dependency to the CQTask, which adds an output DStream (foreach).
    if (!SharkEnv.streams.hasSscStarted(executor)) {
        val ssc = SharkEnv.streams.getSsc(executor)
        val launchTask = TaskFactory.get(
            new StreamingLaunchWork(SharkEnv.streams.getSsc(executor)), conf)

          assert(ssc != null)

        SharkEnv.streams.addStartedSsc(ssc)
        cqTask.addDependentTask(launchTask)
      }
    */

    if (cmd_trimmed.toLowerCase().equals("quit") ||
      cmd_trimmed.toLowerCase().equals("exit") ||
      tokens(0).equalsIgnoreCase("source") ||
      cmd_trimmed.startsWith("!") ||
      tokens(0).toLowerCase().equals("list") ||
      ss.asInstanceOf[CliSessionState].isRemoteMode()) {
      val start = System.currentTimeMillis()
      super.processCmd(cmd)
      val end = System.currentTimeMillis()
      val timeTaken: Double = (end - start) / 1000.0
      console.printInfo("Time taken (including network latency): " + timeTaken + " seconds")
    } else {
      val hconf = conf.asInstanceOf[HiveConf]
      val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hconf)
      if (proc != null) {

        // Spark expects the ClassLoader to be an URLClassLoader.
        // In case we're using something else here, wrap it into an URLCLassLaoder.
        if (System.getenv("TEST_WITH_ANT") == "1") {
          val cl = Thread.currentThread.getContextClassLoader()
          Thread.currentThread.setContextClassLoader(new URLClassLoader(Array(), cl))
        }

        if (proc.isInstanceOf[Driver]) {
          // There is a small overhead here to create a new instance of
          // SharkDriver for every command. But it saves us the hassle of
          // hacking CommandProcessorFactory.
          val execMode = SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE)
          val qp: Driver =
            if (execMode == "shark") {
              new SharkDriver(hconf)
            } else if (execMode == "streaming") {
              new StreamingDriver(hconf)
            } else {
              proc.asInstanceOf[Driver]
            }

          logInfo("Execution Mode: " + SharkConfVars.getVar(conf, SharkConfVars.EXEC_MODE))

          qp.init()
          val out = ss.out
          val start:Long = System.currentTimeMillis()
          if (ss.getIsVerbose()) {
            out.println(cmd)
          }

          ret = qp.run(cmd).getResponseCode()
          if (ret != 0) {
            qp.close()
            return ret
          }

          val res = new ArrayList[String]()

          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER)) {
            // Print the column names.
            val fieldSchemas = qp.getSchema.getFieldSchemas
            if (fieldSchemas != null) {
              out.println(fieldSchemas.map(_.getName).mkString("\t"))
            }
          }

          try {
            while (!out.checkError() && qp.getResults(res)) {
              res.foreach(out.println(_))
              res.clear()
            }
          } catch {
            case e:IOException =>
              console.printError("Failed with exception " + e.getClass().getName() + ":" +
                e.getMessage(), "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e))
              ret = 1
          }

          val cret = qp.close()
          if (ret == 0) {
            ret = cret
          }

          val end:Long = System.currentTimeMillis()
          if (end > start) {
            val timeTaken:Double = (end - start) / 1000.0
            console.printInfo("Time taken: " + timeTaken + " seconds", null)
          }

          // Destroy the driver to release all the locks.
          if (qp.isInstanceOf[SharkDriver]) {
            qp.destroy()
          }

        } else {
          if (ss.getIsVerbose()) {
            ss.out.println(tokens(0) + " " + cmd_1)
          }
          ret = proc.run(cmd_1).getResponseCode()
        }
      }
    }
    ret
  }

  override def processFile(fileName: String): Int = {
    if (Utils.isS3File(fileName)) {
      // For S3 file, fetch it from S3 and pass it to Hive.
      val conf = ss.getConf()
      Utils.setAwsCredentials(conf)
      var bufferReader: BufferedReader = null
      var rc: Int = 0
      try {
        bufferReader = Utils.createReaderForS3(fileName, conf)
        rc = processReader(bufferReader)
        bufferReader.close()
        bufferReader = null
      } finally {
        IOUtils.closeStream(bufferReader)
      }
      rc
    } else {
      // For non-S3 file, just use Hive's processFile.
      super.processFile(fileName)
    }
  }
}
