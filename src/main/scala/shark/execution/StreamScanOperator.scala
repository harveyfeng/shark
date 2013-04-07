package shark.execution

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
import shark.memstore.{TableStats, TableStorage}
import spark.RDD
import spark.rdd.{PartitionPruningRDD, UnionRDD}

import spark.streaming.{Duration, Interval, Time}

/**
 * Replaces TableScanOperator as the TopOperator in Shark operator trees.
 * This fetches the InputDStream source for a table, takes windows if
 * specified, and slices the DStream to get input RDD(s).
 *
 * TODO: Create StreamOperator abstract class, wrap and don't extend SharkOperators
 */
class StreamScanOperator extends TableScanOperator {

  @BeanProperty var tableName: String = _

  // TODO: figure out which vars actually need @BeanProperty
  // Time at which the DStream generates an RDD for the table being
  // scanned.
  @BeanProperty var currentComputeTime: Time = _

  // Seconds specified by LAST.
  @BeanProperty var windowDuration: Duration = _

  // Process the RDD input from the transform function.
  override def execute(): RDD[_] = {
    // Get the inputDStream from the cache. For FileSinkInputDStreams, this should
    // be a transformed inputDStream with HadoopRDDs.
    val inputDStream = SharkEnv.streams.getStream(tableName)

    // Sanity check
    assert(inputDStream != null)

    if (currentComputeTime == null) {
      // "Real-time" query
      currentComputeTime = SharkEnv.streams.getLatestComputeTime(inputDStream)
    } else {
      // A CQ. Update the latest compute time
      SharkEnv.streams.updateComputeTime(inputDStream, currentComputeTime)
    }

    // If there's no window specified, just use the single RDD generated at each slideDuration.
    // Note: not using WindoweDStream, since we would have to create a new one every time the
    // source DStream changes. Easier to just slice it manually.
    val sliceDuration = if (windowDuration == null) inputDStream.slideDuration else windowDuration
    val fromDuration = currentComputeTime - sliceDuration + inputDStream.slideDuration
    val inputRdds = inputDStream.slice(fromDuration, currentComputeTime).asInstanceOf[Seq[RDD[Any]]]
    val unionedInputRDDs = SharkEnv.sc.union(inputRdds)

    // Delegate partition processing to TableScanOperator once we have the duration RDDs.
    val formattedRDD = Operator.executeProcessPartition(this, unionedInputRDDs)

    return formattedRDD
  }
}
