package shark.streaming

import scala.collection.JavaConversions._

import java.util.{List => JavaList}

import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.lib.Node

import shark.SharkEnv
import spark.streaming.Duration


/**
* Functions for traversing ASTs from commands involving streams. In Hive, this part is
* all in SemanticAnalyzer.java
*/
object ASTTraversal {

  // Path from root: TOK_QUERY -> TOK_FROM -> TOK_TABREF -> TOK_TABNAME -> <table name>
  //                                                     -> <alias>
  def processQueryNode(queryTokenNode: ASTNode, context: StreamingCommandContext) {
    def traverseNode(node: ASTNode) {
      node.getToken.getType match {
        case HiveParser.TOK_TABREF => {

          // TODO: use default window durations

          // TODO: use TOK_WINDOW here once parser works.
          // Example for how it works right now:
          // FROM <tablename> window_<duration>
          //val windowString = "window_"
          val aliasIndex = node.getChildren.size - 1
          val alias = node.getChild(aliasIndex).getText
          val tableName =
            BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
          val stream = SharkEnv.streams.getStream(tableName)
          // If it isn't a stream, then we're likely joining with a historical table.
          // If this is a stream, and user hasn't provided window, default to parent Duration
          if (stream != null && context.keyToWindow.contains(tableName)) {
            var duration = SharkEnv.streams.getStream(tableName).slideDuration
            context.keyToWindow.put(tableName, (duration, false))
            val sourceStream = SharkEnv.streams.getStream(tableName)
            context.streamToWindow.put(sourceStream, (duration, false))
          }
        }
        case _ => traverseChildren(node.getChildren)
      }
    }

    def traverseChildren(children: JavaList[Node]) {
      if (children != null) {
        for (child <- children) {
          child match {
            case astNode: ASTNode => traverseNode(astNode)
            case _ => Unit
          }
        }
      }
    }

    // TOK_FROM
    val fromTokenNode = queryTokenNode.getChild(0).asInstanceOf[ASTNode]
    traverseNode(fromTokenNode)
  }
}
