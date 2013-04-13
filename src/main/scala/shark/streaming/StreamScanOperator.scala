package shark.streaming

import java.util.{ArrayList, Arrays}

import scala.reflect.BeanProperty

import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS
import org.apache.hadoop.hive.ql.exec.{TableScanOperator => HiveTableScanOperator}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.Partition
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.plan.{PlanUtils, PartitionDesc, TableDesc}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory,
  StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Writable

import shark.{SharkConfVars, SharkEnv}
import shark.execution.serialization.XmlSerializer
import shark.execution.{Operator, TableScanOperator}
import shark.memstore.{TableStats, TableStorage}
import spark.RDD
import spark.rdd.{PartitionPruningRDD, UnionRDD}

import spark.streaming.{DStream, Duration, Interval, Time}

/**
 * Replaces TableScanOperator as the TopOperator in Shark operator trees.
 * This fetches the InputDStream source for a table, takes windows if
 * specified, and slices the DStream to get input RDD(s).
 *
 * TODO: Create StreamOperator abstract class, wrap and don't extend SharkOperators
 */
class StreamScanOperator extends TableScanOperator {

  @BeanProperty var tableName: String = _

  // TODO: figure out which vars actually need @BeanProperty.
  // Time at which the DStream generates an RDD for the table being
  // scanned.
  @transient var currentComputeTime: Time = _

  // Seconds specified by LAST.
  @transient var windowDuration: Duration = _

  @transient var inputDStream: DStream[_] = _
  @BeanProperty var isIntermediateStream: Boolean = _

  // Initialization in StreamingTask
  def initializeInputStream() {
    super.initializeOnMaster()
    // Get the inputDStream. We must use WindowedDStream, since it
    // sets rememberDuration for dependencies. This is called before SSC
    // starts.
    val stream = SharkEnv.streams.getStream(tableName)
    isIntermediateStream = SharkEnv.streams.isIntermediateStream(inputDStream)

    // Sanity check
    assert(stream != null)

    if (!(windowDuration == null) &&
        (windowDuration.milliseconds == stream.slideDuration.milliseconds)) {
      inputDStream = stream
    } else {
      inputDStream = stream.window(windowDuration)
    }
  }

  // Process the RDD input from the transform function.
  override def execute(): RDD[_] = {
    // Get the inputDStream from the cache. For FileSinkInputDStreams, this should
    // be a transformed inputDStream with HadoopRDDs.

    if (currentComputeTime == null) {
      // "Real-time" query
      currentComputeTime = Time(System.currentTimeMillis)
    }

    // Update the latest compute time
    SharkEnv.streams.updateComputeTime(inputDStream, currentComputeTime)

    val inputRdds = inputDStream.slice(currentComputeTime, currentComputeTime).asInstanceOf[Seq[RDD[Any]]]
    //val unionedInputRDDs = SharkEnv.sc.union(inputRdds)
    val unionedInputRDD = inputRdds.head

    // Delegate partition processing to TableScanOperator once we have the duration RDDs.
    val formattedRDD = Operator.executeProcessPartition(this, unionedInputRDD)

    return formattedRDD
  }
}
