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

import shark.api.TableRDD
import shark.execution
import shark.execution._
import shark.memstore2.ColumnarSerDe
import shark.parse.{SharkSemanticAnalyzer, QueryContext}
import shark.SharkEnv

import org.apache.spark.streaming.{DStream, Duration, StreamingContext}
import org.apache.spark.rdd.RDD

// TODO(harvey): Take in a Shark operator tree and split it into F1-fragments, where stream-stream
//               shuffles are required.


// TODO(harvey): Clean up this terrible code blob.


object StreamingOperatorTreeUtils {

  // (JoinOperator, (JoinTag, ParentOpSink))
  def getFirstStreamJoinOpAndParents(
      operator: Operator[_]
    ): Option[Tuple2[Operator[_], Seq[Tuple2[Int, Operator[_]]]]] = {

    def findJoinOperator(
        naryOp: NaryOperator[_]
      ): Option[Tuple2[Operator[_], Seq[Tuple2[Int, Operator[_]]]]] = {
      var currentOp = naryOp
      while (currentOp.isInstanceOf[NaryOperator[_]]) {
        if (currentOp.isInstanceOf[TopOperator[_]]) {
          return None
        }
        currentOp = currentOp.parentOperators.head.asInstanceOf[NaryOperator[_]]
      }
      if (currentOp.isInstanceOf[JoinOperator]) {
        // Found join operator
        val joinOp = currentOp.asInstanceOf[JoinOperator]
        val parentOps = joinOp.parentOperators.asInstanceOf[Seq[Operator[_]]]
        var numScanOpsFound = 0
        val filteredParentOps = parentOps.filter(op => getParentStreamScanOp(op).isDefined)
        if (filteredParentOps.size == 2) {
          if (filteredParentOps.size != parentOps.size) {
            throw new Exception("Internal Error: Cannot join > 2 streams with other sources")
          } else {
            // Found!
            val res = parentOps.map(op =>
              (op.getTag, op.asInstanceOf[Operator[HiveOperator]])).toSeq
            return Some(joinOp, res)
          }
        } else if (filteredParentOps.size < 2) {
          throw new Exception("Internal Error: Join with less than 2 streaming ops")
        } else if (filteredParentOps.size > 2) {
          throw new Exception("Internal Error: Join with more than 2 streaming ops")
        }
      }
      // Found unary operator
      val parentOps = currentOp.parentOperators
      for (parentOp <- parentOps) {
        val toReturn = findJoinOperator(parentOp.asInstanceOf[NaryOperator[_]])
        if (toReturn.isDefined) {
          return toReturn
        }
      }
      return None
    }
    return findJoinOperator(operator.asInstanceOf[NaryOperator[_]])
  }

  def getParentStreamScanOp(operator: Operator[_]): Option[StreamScanOperator] = {
    var currentOp = operator
    while (!currentOp.isInstanceOf[TopOperator[_]]) {
      currentOp = currentOp.parentOperators.head
    }
    if (currentOp.isInstanceOf[StreamScanOperator]) {
      return Some(currentOp.asInstanceOf[StreamScanOperator])
    } else {
      return None
    }
  }

  def convertTopOpsInSharkTree(
    topOps: Seq[Operator[_]],
    cmdContext: StreamingCommandContext,
    pctx: ParseContext
  ): Seq[(String, DStream[Any])] = {
    // If the any TableScanOperator is for a DStream input source,
    // replace it with StreamScanOperator.
    val topToTable = pctx.getTopToTable
    val inputStreams = new ArrayBuffer[(String, DStream[_])]()

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
            inputStreams.append((tableName, stream))
          }
          case _ => Unit
        }
      }
      topOp.asInstanceOf[shark.execution.TopOperator[_]].tableName = tableName
    }

    return inputStreams.toSeq.asInstanceOf[Seq[(String, DStream[Any])]]
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