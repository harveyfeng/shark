package shark.streaming

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import java.lang.reflect.Method
import java.util.{ArrayList, List => JavaList, Map => JavaMap}

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

import shark.execution.{HiveOperator, Operator, SparkTask, TableScanOperator, TerminalOperator}
import shark.execution.TableRDD
import shark.memstore.ColumnarSerDe
import shark.parse.{SharkSemanticAnalyzer, QueryContext}
import shark.SharkEnv

import spark.streaming.{DStream, Duration, StreamingContext}
import spark.RDD

object StreamingOperatorFactory {
  
  /*
  def createStreamingTreeFromSharkTree(
    terminalOp: TerminalOperator,
    cmdContext: StreamingCommandContext,
    pctx: ParseContext
  ): DStream[Any] = {
    // If the any TableScanOperator is for a DStream input source,
    // replace it with StreamScanOperator.
    val topToTable = pctx.getTopToTable
    val inputStreams = new ArrayBuffer[DStream[_]]()

    for (topOp <- terminalOp.returnTopOperators) {
      val tableName = topToTable.get(topOp.hiveOp).getTableName
      SharkEnv.streams.getStream(tableName) match {
        case stream: DStream[_] => {
          val streamScanOp = convertTableScanToStreamScan(
            topOp.asInstanceOf[TableScanOperator])

          // Do some StreamScanOp initialization...move to CQTask?
          streamScanOp.tableName = tableName
          streamScanOp.windowDuration = cmdContext.streamToWindow.get(tableName)

          // TODO: should we set source stream for each StreamScanOp here?
          //       Depends on whether streams are immutable...
          inputStreams.append(stream)
          cmdContext.streamOps.append(streamScanOp)
        }
        case _ => Unit
      }
    }
    return null
  }
  */
  

  def createStreamingTreeFromSharkTree(
    topOps: Seq[Operator[_]],
    cmdContext: StreamingCommandContext,
    pctx: ParseContext
  ): Seq[DStream[Any]] = {
    // If the any TableScanOperator is for a DStream input source,
    // replace it with StreamScanOperator.
    val topToTable = pctx.getTopToTable
    val inputStreams = new ArrayBuffer[DStream[_]]()

    for (topOp <- topOps) {
      val tableName = topToTable.get(topOp.hiveOp).getTableName
      if (SharkEnv.streams.isStream(tableName)) {
        SharkEnv.streams.getStream(tableName) match {
          case stream: DStream[_] => {
            if (SharkEnv.streams.isInputStream(tableName)) {
              val streamScanOp = convertTableScanToStreamScan(
                topOp.asInstanceOf[TableScanOperator])

              // Do some StreamScanOp initialization...move to CQTask?
              streamScanOp.tableName = tableName
              streamScanOp.windowDuration = cmdContext.keyToWindow.get(tableName)._1

              // TODO: should we set source stream for each StreamScanOp here?
              //       Depends on whether streams are immutable...
              cmdContext.streamOps.append(streamScanOp)
            } else if (SharkEnv.streams.isIntermediateStream(tableName)) {
              // TODO: make a "WindowScanOperator"?
              val hasUserSpecWindow = cmdContext.keyToWindow.get(tableName)._2
              if (hasUserSpecWindow) {
                cmdContext.tableScanOps.append(topOp.asInstanceOf[TableScanOperator])
              }
            }
            inputStreams.append(stream)
          }
        }
      }
      topOp.asInstanceOf[shark.execution.TopOperator[_]].tableName = tableName
    }

    return inputStreams.toSeq.asInstanceOf[Seq[DStream[Any]]]
  }

	
  def convertTableScanToStreamScan(tableScanOp: TableScanOperator): StreamScanOperator = {
    val newOp = _newStreamingOperatorInstance(classOf[StreamScanOperator], tableScanOp)
      _replaceOpInTree(tableScanOp, newOp)
    return newOp.asInstanceOf[StreamScanOperator]
  }

  private def _replaceOpInTree(originalOp: Operator[_], newOp: Operator[_]) {
    for (childOp <- originalOp.childOperators) {
      val parents = childOp.parentOperators
      var i = 0
      while (i < parents.size) {
        if (parents(i) == originalOp) parents.remove(i)
        i += 1
      }
      childOp.addParent(newOp)
    }
    for (parentOp <- originalOp.parentOperators) {
      var children = parentOp.childOperators
      var i = 0
      while (i < children.size) {
        if (children(i) == originalOp) children.remove(i)
        i += 1
      }
      parentOp.addChild(newOp)
    }
  }

  private def _newStreamingOperatorInstance[T <: HiveOperator](
    cls: Class[_ <: Operator[T]], sharkOp: Operator[T]): Operator[_] = {
    val newOp = cls.newInstance()
    newOp.hiveOp = sharkOp.hiveOp.asInstanceOf[T]
    return newOp
  }
}