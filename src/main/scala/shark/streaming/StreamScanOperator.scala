package shark.streaming

import java.util.{ArrayList, Arrays}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.BeanProperty

import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS
import org.apache.hadoop.hive.ql.exec.{TableScanOperator => HiveTableScanOperator}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.Partition
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.plan.{PlanUtils, PartitionDesc, TableDesc}
import org.apache.hadoop.hive.serde2.`lazy`.objectinspector.LazySimpleStructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory,
  StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.{Text, Writable}

import shark.{SharkConfVars, SharkEnv}
import shark.execution.serialization.XmlSerializer
import shark.execution.{Operator, TableScanOperator}
import shark.memstore.{TableStats, TableStorage}
import spark.RDD
import spark.rdd.{PartitionPruningRDD, UnionRDD}

import spark.streaming.{DStream, Duration, Interval, Time}

/**
 * Replaces TableScanOperator as the TopOperator in Shark operator trees.
 * This fetches the InputDStream source for a table, and slices the DStream
 * to get input RDD(s).
 *
 * TODO: Create StreamOperator abstract class, wrap and don't extend SharkOperators
 */
class StreamScanOperator extends TableScanOperator {

  @BeanProperty var tableName: String = _

  // TODO: figure out which vars actually need @BeanProperty.
  // Time at which the DStream generates an RDD for the table being
  // scanned, OR time of a "real time" query.
  @BeanProperty var currentComputeTime: Long = _

  // Seconds specified by LAST
  @transient var windowDuration: Duration = _

  @transient var inputDStream: DStream[_] = _

  @transient var inputRdd: RDD[_] = _

  @BeanProperty var separator: Byte = _

  // Initialization in StreamingTask, after TableScanOp is initialized
  def initializeInputStream() {
    super.initializeOnMaster()
    separator = inputObjectInspectors(0).asInstanceOf[LazySimpleStructObjectInspector].getSeparator
    // Get the inputDStream. We must use WindowedDStream, since it
    // sets rememberDuration for dependencies. This is called before SSC
    // starts.
    inputDStream = SharkEnv.streams.getStream(tableName)

    // Sanity check
    assert(inputDStream != null)
  }

  // Process the RDD input from the transform function.
  override def execute(): RDD[_] = {
    // Get the inputDStream from the cache. For FileSinkInputDStreams, this should
    // be a transformed inputDStream with HadoopRDDs.

    if (currentComputeTime == 0L) {
      // "Real-time" query
      currentComputeTime = System.currentTimeMillis
    }

    // Update the latest compute time
    SharkEnv.streams.updateComputeTime(inputDStream, Time(currentComputeTime))

    //val inputRdds = inputDStream.slice(currentComputeTime, currentComputeTime).asInstanceOf[Seq[RDD[Any]]]
    //val unionedInputRDDs = SharkEnv.sc.union(inputRdds)
    //val unionedInputRDD = inputRdds.head

    // Delegate partition processing to TableScanOperator once we have the duration RDDs.
    // Note: op.processPartition => deserializer.deserialize(v)
    val rddPreprocessed = if (SharkEnv.streams.isInputStream(tableName)) preprocessRdd(inputRdd) else inputRdd
    val formattedRDD = Operator.executeProcessPartition(this, rddPreprocessed)

    return formattedRDD
  }

  override def preprocessRdd(rdd: RDD[_]): RDD[_] = {
    // TODO: figure out how Java serialization propagates...
    //val separator2 = separator
    //val currentComputeTime2 = currentComputeTime
    var bytes = ((currentComputeTime / 1000).toString).getBytes
    val byteBuffer = new ArrayBuffer[Byte]()
    byteBuffer.append(separator)
    byteBuffer ++= bytes
    bytes = byteBuffer.toArray

    rdd.mapPartitions { part =>
      part.map { tup =>
        // TODO: use set() that takes byte array
        tup.asInstanceOf[Text].append(bytes, 0, bytes.length)
        tup
      }
    }
  }
}
