package shark.streaming

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan.api.StageType
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.session.SessionState

import shark.execution.{SparkTask, TableScanOperator, TableRDD}
import shark.{LogHelper, SharkEnv}

import spark.RDD
import spark.streaming.{Duration, DStream, StreamingContext, Time}


class StreamingLaunchWork(val ssc: StreamingContext) extends java.io.Serializable

/**
 * StreamingLaunchTask starts the StreamingContext.
 * 
 * TODO: use TaskContext.executeOnCompleteCallbacks()?
 */
class StreamingLaunchTask extends org.apache.hadoop.hive.ql.exec.Task[StreamingLaunchWork]
  with java.io.Serializable with LogHelper {

  override def execute(driverContext: DriverContext): Int = {
    logInfo("Executing " + this.getClass.getName)

    work.ssc.start()

    0
  }

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-SPARK-STREAMING-LAUNCH"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit

}

class CQWork(
    val cmdContext: StreamingCommandContext,
    val sparkTask: SparkTask,
    val executor: DStream[_])
  extends java.io.Serializable


class CQTask extends org.apache.hadoop.hive.ql.exec.Task[CQWork]
  with java.io.Serializable with LogHelper {

  var isInitialized = false

  override def initialize(conf: HiveConf, queryPlan: QueryPlan, driverContext: DriverContext) {
    super.initialize(conf, queryPlan, driverContext)
    work.sparkTask.initialize(conf, queryPlan, driverContext)
    for (childTask <- work.sparkTask.getChildTasks) {
      childTask.initialize(conf, queryPlan, driverContext)
    }
  }

  override def execute(driverContext: DriverContext): Int = {
    val cmdContext = work.cmdContext
    val sparkTask = work.sparkTask

    val terminalOp = sparkTask.getWork.terminalOperator
    val tableScanOps = terminalOp.returnTopOperators().asInstanceOf[Seq[TableScanOperator]]

    for (streamScanOp <- cmdContext.streamOps) {
      streamScanOp.initializeInputStream()
    }

    // If the executor needs a window...
    val executor = getExecutor(cmdContext.streamOps, cmdContext.duration)

    val cq = (rdd: RDD[_], time: Time) => {
      for (streamScanOp <- cmdContext.streamOps) {
        streamScanOp.currentComputeTime = time
      }
      var tableRdd: TableRDD = null
      // Execute main query.
      if (isInitialized) {
        // Initialize tableScanDesc every time because it sets
        // table partition metadata.
        sparkTask.initializeTableScanTableDesc(tableScanOps)
        val sinkRdd = terminalOp.execute().asInstanceOf[RDD[Any]]
        tableRdd = new TableRDD(sinkRdd, sparkTask.getWork.resultSchema, terminalOp.objectInspector)
      } else {
        sparkTask.executeTask()
        tableRdd = sparkTask.tableRdd

        isInitialized = true
      }
      // Execute dependencies
      for (childTask <- sparkTask.getChildTasks) {
        childTask.executeTask()
      }

      tableRdd
    }

    if (cmdContext.isDerivedStream) {
      val transformed = executor.transform(cq)
      SharkEnv.streams.putIntermediateStream(cmdContext.tableName, transformed, executor)
    } else if (cmdContext.isArchiveStream) {
      executor.foreach((rdd, time) => cq(rdd, time))
    }
    0
  }

  def getExecutor(scanOps: Seq[StreamScanOperator], duration: Duration): DStream[_] = {
    // Use the DStream with smallest slideDuration.
    val sourceDStream = scanOps.sortWith((a, b) => a.inputDStream.slideDuration < b.inputDStream.slideDuration).head
    // If the user provides a batch duration and there are > 1 sources, trust that
    // it will be a valid duration (for now)
    if (duration == null || duration == sourceDStream.inputDStream.slideDuration) {
      return sourceDStream.inputDStream
    } else {
      sourceDStream.inputDStream.window(duration, duration)
    }
  }

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-SPARK-STREAMING"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit
}

