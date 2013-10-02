package shark.streaming

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan.api.StageType
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.session.SessionState

import shark.api.TableRDD
import shark.execution._
import shark.execution.serialization._
import shark.{LogHelper, SharkEnv}

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, DStream, StreamingContext, Time}


trait StreamingTask extends java.io.Serializable

class StreamingLaunchWork(val ssc: StreamingContext, val shouldStart: Boolean) extends java.io.Serializable

/**
 * StreamingLaunchTask starts the StreamingContext.
 * 
 * TODO: use TaskContext.executeOnCompleteCallbacks()?
 */
class StreamingLaunchTask extends org.apache.hadoop.hive.ql.exec.Task[StreamingLaunchWork]
  with LogHelper {

  override def execute(driverContext: DriverContext): Int = {
    logInfo("Executing " + this.getClass.getName)

    if (work.shouldStart) {
      work.ssc.start
      SharkEnv.streams.addStartedSsc(work.ssc)
    } else {
      work.ssc.stop
    }

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
  with LogHelper {

  var isInitialized = false

  override def initialize(conf: HiveConf, queryPlan: QueryPlan, driverContext: DriverContext) {
    super.initialize(conf, queryPlan, driverContext)
    work.sparkTask.initialize(conf, queryPlan, driverContext)
    if (work.sparkTask.getChildTasks != null) {
      for (childTask <- work.sparkTask.getChildTasks) {
        childTask.initialize(conf, queryPlan, driverContext)
      }
    }
  }

  override def execute(driverContext: DriverContext): Int = {
    logInfo("Executing " + this.getClass.getName)
    logInfo("Executing task for command: " + work.cmdContext.getCmd)
    val cmdContext = work.cmdContext
    val sparkTask = work.sparkTask

    val terminalOp = sparkTask.getWork.terminalOperator
    val tableScanOps = terminalOp.returnTopOperators().asInstanceOf[Seq[TableScanOperator]]

    Operator.hconf = conf
    sparkTask.initializeTableScanTableDesc(tableScanOps)
    sparkTask.initializeAllHiveOperators(terminalOp)

    sparkTask.isSubTask = true

    for (streamScanOp <- cmdContext.streamOps) {
      streamScanOp.initializeInputStream()
    }

    // If the executor needs a window...
    val executor = work.executor

    // TODO(harvey): Change this to 'topOps'.
    val cq = (rdd: RDD[_], time: Time) => {
      for (streamScanOp <- cmdContext.streamOps) {
        streamScanOp.currentComputeTime = time.milliseconds
        streamScanOp.inputRdd = rdd
      }

      for (tableScanOp <- cmdContext.tableScanOps) {
        tableScanOp.inputUnionRdd = rdd
      }

      var retRdd: RDD[Any] =
        // Execute main query.
        if (isInitialized) {
          // Initialize tableScanDesc every time because it sets
          // table partition metadata.
          sparkTask.initializeTableScanTableDesc(tableScanOps)
          val sinkRdd = terminalOp.execute().asInstanceOf[RDD[Any]]
          new TableRDD(
            sinkRdd,
            sparkTask.getWork.resultSchema,
            terminalOp.objectInspector,
            -1 /* limit */).asInstanceOf[RDD[Any]]
        } else {
          sparkTask.executeTask()
          isInitialized = true
          // NOTE: See TODO in SparkTask.
          sparkTask.tableRdd.get.prev
        }

      // Execute dependencies
      if (sparkTask.getChildTasks != null) {
        for (childTask <- sparkTask.getChildTasks) {
          childTask.executeTask()
        }
      }

      retRdd
    }
    
    if (SharkEnv.streams.hasSscStarted(executor)) {
      sparkTask.executeTask()
      // Execute dependencies
      if (sparkTask.getChildTasks != null) {
        for (childTask <- sparkTask.getChildTasks) {
          childTask.executeTask()
        }
      }
    }

    if (cmdContext.isDerivedStream) {
      val transformed = executor.transform(cq).persist(StorageLevel.MEMORY_ONLY_SER)
      SharkEnv.streams.putIntermediateStream(cmdContext.tableName, transformed, executor)
      transformed.foreach(_ => Unit)
    } else if (cmdContext.isArchiveStream) {
      val tmp = 0
      executor.foreach((rdd, time) => cq(rdd, time))
    }
    0
  }

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-SPARK-STREAMING"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit
}

