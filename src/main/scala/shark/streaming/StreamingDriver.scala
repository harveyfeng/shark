package shark.streaming

import java.util.{ArrayList => JavaArrayList, List => JavaList, Date}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hadoop.hive.ql.{Context, Driver, QueryPlan}
import org.apache.hadoop.hive.ql.exec._
import org.apache.hadoop.hive.ql.exec.OperatorFactory.OpTuple
import org.apache.hadoop.hive.ql.log.PerfLogger
import org.apache.hadoop.hive.ql.metadata.AuthorizationException
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.{SerDe, SerDeUtils}
import org.apache.hadoop.util.StringUtils

import shark.api.TableRDD
import shark.execution.{SparkTask, SparkWork}
import shark.memstore2.ColumnarSerDe
import shark.parse.{QueryContext, SharkSemanticAnalyzerFactory}
import shark.{LogHelper, SharkEnv, SharkDriver}

import org.apache.spark.streaming.{StreamingContext, Duration, Seconds}


/**
* A driver to compile and execute streaming queries (i.e those using EVERY) for Shark.
* The StreamingDriver executes the DStreamTask that starts the StreamingContext.
* SharkStreamingDriver delegates normal queries to SharkDriver.
*/
class StreamingDriver(conf: HiveConf) extends SharkDriver(conf) with LogHelper {

  /**
   * Overload compile to use StreamingSemanticAnalyzer, though this could probably be done by
   * just modifying SemanticAnalyzerFactory...
   */
  override def compile(cmd: String, resetTaskIds: Boolean): Int = {
    val perfLogger: PerfLogger = PerfLogger.getPerfLogger()
    perfLogger.PerfLogBegin(LOG, PerfLogger.COMPILE)

    //holder for parent command type/string when executing reentrant queries
    val queryState = new SharkDriver.QueryState

    if (plan != null) {
      close()
      plan = null
    }

    if (resetTaskIds) {
      TaskFactory.resetId()
    }
    saveSession(queryState)

    try {
      var command = new VariableSubstitution().substitute(conf, cmd)
      context = new StreamingCommandContext(conf, useTableRddSink)
      context.setCmd(command)
      context.setTryCount(getTryCount())


      val cmdContext = context.asInstanceOf[StreamingCommandContext]

      command = StreamingDriver.preprocessCommand(command, cmdContext)

      context.setCmd(command)

      var tree: ASTNode = null
      var sem: BaseSemanticAnalyzer = null
      if (command.toLowerCase.startsWith("start") || command.toLowerCase.startsWith("stop")) {
        sem = new StreamingSemanticAnalyzer(conf)
      } else {
        tree = ParseUtils.findRootNonNullToken((new ParseDriver()).parse(command, context))
        sem = SharkSemanticAnalyzerFactory.get(conf, tree)
      }

      // Do semantic analysis and plan generation
      val saHooks = SharkDriver.saHooksMethod.invoke(this, HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK,
        classOf[AbstractSemanticAnalyzerHook]).asInstanceOf[JavaList[AbstractSemanticAnalyzerHook]]
      // Note: this is never null, but can be empty.
      if (!saHooks.isEmpty) {
        val hookCtx = new HiveSemanticAnalyzerHookContextImpl()
        hookCtx.setConf(conf)
        saHooks.foreach(_.preAnalyze(hookCtx, tree))
        sem.analyze(tree, context)
        hookCtx.update(sem)
        saHooks.foreach(_.postAnalyze(hookCtx, sem.getRootTasks()))
      } else {
        sem.analyze(tree, context)
      }

      logInfo("Semantic Analysis Completed")

      sem.validate()

      plan = new QueryPlan(command, sem, perfLogger.getStartTime(PerfLogger.DRIVER_RUN))

      // Initialize FetchTask right here. Somehow Hive initializes it twice...
      if (sem.getFetchTask != null) {
        sem.getFetchTask.initialize(conf, plan, null)
      }

      // get the output schema
      schema = Driver.getSchema(sem, conf)

      // skip the testing serialization code

      // do the authorization check
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
        try {
          perfLogger.PerfLogBegin(LOG, PerfLogger.DO_AUTHORIZATION)
          // Use reflection to invoke doAuthorization().
          SharkDriver.doAuthMethod.invoke(this, sem)
        } catch {
          case authExp: AuthorizationException => {
            logError("Authorization failed:" + authExp.getMessage()
              + ". Use show grant to get more details.")
            return 403
          }
        } finally {
          perfLogger.PerfLogEnd(LOG, PerfLogger.DO_AUTHORIZATION)
        }
      }

      // Success!
      0
    } catch {
      case e: SemanticException => {
        errorMessage = "FAILED: Error in semantic analysis: " + e.getMessage()
        logError(errorMessage, "\n" + StringUtils.stringifyException(e))
        10
      }
      case e: ParseException => {
        errorMessage = "FAILED: Parse Error: " + e.getMessage()
        logError(errorMessage, "\n" + StringUtils.stringifyException(e))
        11
      }
      case e: Exception => {
        errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e)
        logError(errorMessage, "\n" + StringUtils.stringifyException(e))
        12
      }
    } finally {
      perfLogger.PerfLogEnd(LOG, PerfLogger.COMPILE)
      restoreSession(queryState)
    }
  }
}


object StreamingDriver {
  import scala.util.matching.Regex
  import scala.util.matching.Regex.Match

  def preprocessCommand(cmd: String, cmdContext: StreamingCommandContext): String = {
    var command = cmd

    // TODO: remove after parsing works.


    // If this is archiving a stream, INSERT INTO TABLE ... SELECT ... FROM ... BATCH ... SECONDS
    if (command.toLowerCase.contains("insert") &&
        command.toLowerCase.contains("batch") &&
        command.toLowerCase.contains("seconds")) {
      // Get the duration (BATCH/EVERY x SECONDS ... )
      val batchIndex = command.indexOf("batch")
      val batchSubstring = command.substring(batchIndex, command.length)
      val openQuoteIndex = batchSubstring.indexOf("'")
      val closeQuoteIndex = batchSubstring.indexOf("'", openQuoteIndex + 1)
      val batchDuration = batchSubstring.substring(openQuoteIndex + 1, closeQuoteIndex).trim
      // Just use seconds for now
      val batchDurationSeconds = (batchDuration.split(' ')(0).toLong) * 1000
      cmdContext.duration = Duration(batchDurationSeconds)

      // Cut off the BATCH part.
      command = command.substring(0, batchIndex)

      cmdContext.isArchiveStream = true

      // In StreamingSemanticAnalyzer, get the window specs for each source table
    }

    // If this is a stream creation, CREATE STREAM <schema> TBLPROPERTIES("batch"=<...>, "path"=<...>);
    // NEW: DERIVE STREAM [IF NOT EXISTS] stream AS SELECT ... [BATCH interval]
    if (command.toLowerCase.contains("derive stream") || command.toLowerCase.contains("create stream") ) {
      // Rewrite using CREATE TABLE
      command = "create table " + command.substring(14, command.length)
      cmdContext.isCreateStream = true

      // BATCH interval
      // Get the duration (BATCH/EVERY x SECONDS ... )
      val batchIndex = command.toLowerCase.indexOf("batch")
      val batchSubstring = command.substring(batchIndex, command.length)
      val openQuoteIndex = batchSubstring.indexOf("'")
      val closeQuoteIndex = batchSubstring.indexOf("'", openQuoteIndex + 1)
      val batchDuration = batchSubstring.substring(openQuoteIndex + 1, closeQuoteIndex).trim

      // Just use seconds for now
      val batchDurationSeconds = (batchDuration.split(' ')(0).toLong) * 1000
      cmdContext.duration = Duration(batchDurationSeconds)

      // Cut off the BATCH part.
      command = command.substring(0, batchIndex)

      // If it has a READ DIRECTORY dir_name
      if (command.toLowerCase.contains("read directory")) {
        if (batchSubstring == "") {
          throw new SemanticException("Must include BATCH interval in CREATE STREAM")
        }
        val readDirIndex = command.toLowerCase.indexOf("as read directory")
        val readDirSubstring = command.substring(readDirIndex, command.length)
        val openQuoteIndex = readDirSubstring.indexOf("'")
        val closeQuoteIndex = readDirSubstring.indexOf("'", openQuoteIndex + 1)

        val readDirectoryStr = readDirSubstring.substring(openQuoteIndex + 1, closeQuoteIndex).trim
        cmdContext.readDirectory = readDirectoryStr

        // Cut off the READ DIRECTORY part
        command = command.substring(0, readDirIndex)
      }

      // For derived streams
      if (command.toLowerCase.contains("as select")) {
        val splitIndex = command.toLowerCase.indexOf("as select")
        command = command.substring(0, splitIndex) +
          " ROW FORMAT SERDE 'shark.memstore2.ColumnarSerDe' " +
          " TBLPROPERTIES('shark.cache'='true', 'shark.cache.storageLevel'='MEMORY_ONLY_SER') " +
          command.substring(splitIndex, command.length)
      }
    }

    if (command.contains("last") && command.contains("seconds")) {
      val regx = new Regex("""last +'([0-9a-zA-Z ]+)' +of +([a-zA-Z0-9\_]+)""", "window", "stream")
      val tup = regx findFirstIn command match {
        case Some(regx(window, stream)) => Some (window, stream)
        case None => None
      }
      val (windowStr, name) = tup.get
      val windowDuration = Duration(windowStr.split(' ')(0).toLong) * 1000
      cmdContext.keyToWindow.put(name, (windowDuration, true))
      val sourceStream = SharkEnv.streams.getStream(name)
      cmdContext.streamToWindow.put(sourceStream, (windowDuration, true))
      command = regx replaceAllIn ( command, (m: Match) =>
        "%s" format (m group "stream")
      )
    }

    return command
  }

}




