package shark

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

import shark.execution.{CQWork, CQTask, SparkTask, SparkWork, StreamingLaunchWork,
  StreamingLaunchTask, TableRDD}
import shark.memstore.ColumnarSerDe
import shark.parse.{QueryContext, SharkSemanticAnalyzerFactory, StreamingCommandContext}

import spark.streaming.{StreamingContext, Duration, Seconds}


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
      context.setCmd(cmd)
      context.setTryCount(getTryCount())

      // TODO: remove after parsing works.
      // If this is archiving a stream, INSERT INTO TABLE ... SELECT ... FROM ... EVERY ... SECONDS
      val cmdContext = context.asInstanceOf[StreamingCommandContext]
      if (command.toLowerCase.contains("every")) {
        // Get the duration (BATCH/EVERY x SECONDS ... )
        val openQuoteIndex = command.indexOf("'")
        val closeQuoteIndex = command.indexOf("'", openQuoteIndex + 1)
        val batchDuration = command.substring(openQuoteIndex + 1, closeQuoteIndex).trim
        // Just use seconds for now
        val batchDurationSeconds = (batchDuration.split(' ')(0).toLong) * 1000
        cmdContext.duration = Duration(batchDurationSeconds)

        // Cut off the EVERY part.
        command = command.substring(closeQuoteIndex + 1, command.length)

        // In StreamingSemanticAnalyzer, get the window specs for each source table
      }
      // If this is a stream creation, CREATE STREAM ...
      if (command.toLowerCase.contains("create stream")) {
        command = command.replaceFirst("stream", "table")
        cmdContext.isCreateStream = true
        // Batch duration, source path, will be read from TBLPROPERTIES("batch"=..., "directory"...)
      }

      val tree = ParseUtils.findRootNonNullToken((new ParseDriver()).parse(command, context))
      val sem = SharkSemanticAnalyzerFactory.get(conf, tree)

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
