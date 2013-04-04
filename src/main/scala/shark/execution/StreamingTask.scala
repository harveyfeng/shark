package shark.execution

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan.api.StageType
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.session.SessionState

import shark.{LogHelper, SharkEnv}
import shark.parse.StreamingCommandContext

import spark.RDD
import spark.streaming.{DStream, StreamingContext, Time}


class StreamingLaunchWork(val ssc: StreamingContext) extends java.io.Serializable

/**
 * StreamingLaunchTask starts the StreamingContext that will execute PQ plans
 * composed of RDD and stream operators.
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

  override def initialize(conf: HiveConf, queryPlan: QueryPlan, driverContext: DriverContext) {
    work.sparkTask.initialize(conf, queryPlan, driverContext)
  }

  override def execute(driverContext: DriverContext): Int = {
    val cmdContext = work.cmdContext
    val sparkTask = work.sparkTask
    val executor = work.executor

    val cq = (rdd: RDD[_], time: Time) => {
      for (streamScanOp <- cmdContext.streamOps) {
        streamScanOp.currentComputeTime = time
      }
      // Execute main query
      sparkTask.executeTask()
      // Execute dependencies
      for (childTask <- sparkTask.getChildTasks) {
        childTask.executeTask()
      }

      val tableRdd = sparkTask.tableRdd

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

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-SPARK-STREAMING"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit
}

