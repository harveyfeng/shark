package shark.streaming

import java.util.{HashMap => JavaHashMap}

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration

import shark.execution.{SparkTask, TableScanOperator}
import shark.parse.QueryContext

import spark.streaming.{DStream, Duration}

/**
 * Some metadata needed to drive continuous query execution.
 */
class StreamingCommandContext(
  	val conf: Configuration,
  	var useTableRddSinkOp: Boolean)
	extends QueryContext(conf, useTableRddSinkOp) {

	// If command is a CREATE STREAM or CREATE STREAM AS
	var isCreateStream: Boolean = _
	var isDerivedStream: Boolean = _
	var isArchiveStream: Boolean = _

	// User-specified READ DIRECTORY for CREATE STREAM
	var readDirectory: String = _

	// User-specified BATCH.
	// Note: only used for archiving right now.
	var duration: Duration = _
	// If isCreateStream, the name of the stream being created.
	// If isArchiveStream, the name of the table being updated.
	// This is just for convenience for now.
	var tableName: String =_
	// Window specification for each DStream source.
	val keyToWindow = new JavaHashMap[String, (Duration, Boolean)]()
	val streamToWindow = new JavaHashMap[DStream[_], (Duration, Boolean)]()
	val streamOps = new ArrayBuffer[StreamScanOperator]()

	// For derived streams on windowed derived streams
	val tableScanOps = new ArrayBuffer[TableScanOperator]()
}
