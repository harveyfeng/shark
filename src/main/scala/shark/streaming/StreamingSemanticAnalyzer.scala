package shark.streaming

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import java.lang.reflect.Method
import java.util.{ArrayList, List => JavaList, HashMap => JavaHashMap, Map => JavaMap}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{FieldSchema, MetaException}
import org.apache.hadoop.hive.metastore.Warehouse
import org.apache.hadoop.hive.ql.exec.{DDLTask, FetchTask, MoveTask, Task, TaskFactory}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.optimizer.Optimizer
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.hive.ql.session.SessionState

import shark.api.TableRDD
import shark.execution.{HiveOperator, Operator, SparkTask, TableScanOperator, TerminalOperator}
import shark.execution.OperatorFactory
import shark.memstore2.ColumnarSerDe
import shark.parse.{SharkSemanticAnalyzer, QueryContext}
import shark.SharkEnv

import org.apache.spark.streaming.{DStream, Duration, StreamingContext}
import org.apache.spark.rdd.RDD


// TODO: Needs better abstraction
class StreamingSemanticAnalyzer(conf: HiveConf) extends SharkSemanticAnalyzer(conf) {

  override def analyzeInternal(ast: ASTNode): Unit = {
    reset()

    val qb = new QB(null, null, false)
    var pctx = getParseContext()
    pctx.setQB(qb)
    pctx.setParseTree(ast)
    init(pctx)

    val cmdContext = pctx.getContext().asInstanceOf[StreamingCommandContext]
    var isCTAS = false

    if (cmdContext.getCmd.trim.toLowerCase.equals("start")) {
      // If the StreamingContext used for this executor DStream hasn't been started, add a
      // StreamingLaunchTask as a dependency to the CQTask, which adds an output DStream (foreach).
        val ssc = SharkEnv.streams.getSscs(0)
        val launchTask = TaskFactory.get(
          new StreamingLaunchWork(ssc, true), conf)

        assert(ssc != null)

        SharkEnv.streams.addStartedSsc(ssc)
        rootTasks.add(launchTask)
        return
    } else if (cmdContext.getCmd.trim.toLowerCase.equals("stop")) {
        val ssc = SharkEnv.streams.getSscs(0)
        val launchTask = TaskFactory.get(
          new StreamingLaunchWork(ssc, false), conf)

        assert(ssc != null)

        SharkEnv.streams.addStartedSsc(ssc)
        rootTasks.add(launchTask)
        return
    }

    logInfo("Starting Shark Streaming Semantic Analysis")

    // if (ast.getToken().getType() == SharkParser.TOK_CREATESTREAM) isCreateStream = true
    // Analyze CREATE TABLE command
    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
      // Note: this means streams are tables...
      // super.analyzeInternal(ast)
      for (ch <- ast.getChildren.asInstanceOf[JavaList[ASTNode]]) {
        ch.getToken.getType match {
          case HiveParser.TOK_QUERY => {
            // Fill cmdContext with metadata info, such as stream <-> window mapping.
            ASTTraversal.processQueryNode(ch, cmdContext)
            isCTAS = true
          }
          case _ =>
            Unit
        }
      }

      // TODO: temporary
      // if streaming, get data needed to create DStreams
      if (cmdContext.isCreateStream) {
        if (isCTAS) {
          // CSAS.
          // Get the query plan from SharkSemanticAnalyzer.
          // This is a DStream transform, so we use CacheSinkOperator to gather stats.
          //super.analyzeInternal(ast)
          this.ctx = new QueryContext(conf, false)

        } else {
          // TODO: Use a StreamDesc, parent CreateTableDesc?

          super.analyzeInternal(ast)
          // SemanticAnalyzer's td is null. Get it from DDLWork.
          val td = rootTasks.head.getWork.asInstanceOf[DDLWork].getCreateTblDesc
          analyzeCreateStream(td, cmdContext)
          return
        }
      } else {
        // Regular CREATE TABLE/CTAS
        super.analyzeInternal(ast)
        return
      }
    } else {
      // This is a query. Still need to check for table sources that are streams.
      SessionState.get().setCommandType(HiveOperation.QUERY)
      ASTTraversal.processQueryNode(ast, cmdContext)
    }

    // Generate Shark SparkTasks and get parse info.
    super.analyzeInternal(ast)
    pctx = getParseContext()

    // Is there a stream source?
    if (cmdContext.keyToWindow.size == 0) {
      if (cmdContext.isCreateStream && isCTAS) {
        throw new SemanticException(
          "Must include at least one stream source for creating a derived stream")
      } else {
        return
      }
    }

    // At this point, the command is either a CSAS, a real-time query that involves
    // stream(s), or an "archive" command.

    // Get the tasks generated by SharkSemanticAnalyzer.
    // There's one SparkTask created for each TerminalOperator.
    var sparkTasks = rootTasks.asInstanceOf[JavaList[SparkTask]]

    // Find all stream source TableScanOperators, and convert them to StreamScanOperators.
    // TODO: The populate operator metadata related to streams is added here, but maybe
    // should be done during Task initialization.
    if (sparkTasks.size == 1) {

      if (cmdContext.isCreateStream && isCTAS) {
        // If CSAS, add the transformedDStream to metadata
        val td = pctx.getQB.getTableDesc
        cmdContext.tableName = td.getTableName

        // TODO: pass this through cmdContext at parsing stage.
        val tblProps = td.getTblProps
        // Seconds

        cmdContext.isDerivedStream = true
      }

      val inputStreams =
        StreamingOperatorFactory.createStreamingTreeFromSharkTree(
          sparkTasks.head.getWork.terminalOperator.returnTopOperators,
          cmdContext,
          pctx)

      // Note: only one input stream right now
      val is = inputStreams(0)
      if (SharkEnv.streams.hasSscStarted(is)) {
        return
      }

      genStreamingTask(cmdContext, inputStreams, sparkTasks.head)

    } else {
      // Don't support mutiple SparkTasks created from condensed DDLs (ex. multi-insert).
      throw new SemanticException(
        "Can't do multiple SparkTask plan generation in streaming mode yet")
    }

    // ================
    // For debugging
    SharkEnv.streams.addCmdContext(cmdContext)
    // ================
    logInfo("Completed streaming plan generation")
  }

  // Create input stream for given table.
  def analyzeCreateStream(td: CreateTableDesc, cmdContext: StreamingCommandContext) {
    val tblProps = td.getTblProps()

    // Stream name
    val tableName = td.getTableName
    // Use seconds for now
    val duration = cmdContext.duration
    val readDirectory = cmdContext.readDirectory

    if (td.getInputFormat.isInstanceOf[org.apache.hadoop.mapred.TextInputFormat]) {
      throw new SemanticException(
        "Shark streaming only supports TextInputFormat for Hive-based file streams")
    }
    // This creates the FileInputStream and adds it to metadata.
    SharkEnv.streams.createFileStream(tableName, readDirectory, duration)
  }

  // TODO: rewrite.the StreamingTask should just register the TerminalStream
  // with the StreamingContext.
  def genStreamingTask(
      cmdContext: StreamingCommandContext,
      sourceDStreams: Seq[DStream[Any]],
      sparkTask: SparkTask
    ) {

    rootTasks.clear()

    val streams = SharkEnv.streams

    val executor = getExecutor(sourceDStreams, cmdContext)
    // Create the CQTask
    val cqTask = TaskFactory.get(new CQWork(cmdContext, sparkTask, executor), conf)

    if (cmdContext.isDerivedStream) {
      // Tasks created by Shark:
      // SparkTask -> {MoveTask, DDLTask}
      // Discard the MoveTask. Replace SparkTask with CQTask.
      // New plan: CQTask -> DDLTask
      val createTblTask = sparkTask.getChildTasks.get(1)
      cqTask.addDependentTask(createTblTask)
      val oldChildTasks = sparkTask.getChildTasks
      while (!oldChildTasks.isEmpty) {
        sparkTask.removeDependentTask(oldChildTasks.head)
      }
      val createTblDesc = createTblTask.getWork.asInstanceOf[DDLWork].getCreateTblDesc

      // SerDe is the same as that used by cached tables.
      createTblDesc.setSerName(classOf[ColumnarSerDe].getName)

    }
    rootTasks.add(cqTask)
  }

  def getExecutor(sourceDStreams: Seq[DStream[Any]], cmdContext: StreamingCommandContext): DStream[Any] = {
    // Use the DStream with smallest slideDuration.
    var executor = sourceDStreams.sortWith(_.slideDuration < _.slideDuration).head
    // If the user provides a batch duration from tblProps, and there are > 1 sources, trust that
    // it will be a valid duration (for now)
    val batchDuration = cmdContext.duration

    // If there is a window on the source stream, take the window, and add a
    // transformedDStream that will update the CacheManager with UnionRDDs that have stats.
    // Note: this should always be true. Default window duration will be batch duration of parent stream.
    val (windowDuration, hasUserSpecWindow) = cmdContext.streamToWindow.get(executor)
    executor =
    if (batchDuration == null) {
      executor.window(windowDuration)
    } else {
      executor.window(windowDuration, batchDuration)
    }
    return executor
  }
}
