package shark.parse

import scala.collection.JavaConversions._

import java.util.{List => JavaList}

import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.lib.Node

import spark.streaming.Duration


/**
* Functions for traversing ASTs from commands involving streams. In Hive, this part is
* all in SemanticAnalyzer.java
*/
object StreamASTTraversal {

  // Path from root: TOK_QUERY -> TOK_FROM -> TOK_TABREF -> TOK_TABNAME -> <table name>
  //                                                     -> <alias>
  def processQueryNode(queryTokenNode: ASTNode, context: StreamingCommandContext) {
    def traverseNode(node: ASTNode) {
      node.getToken.getType match {
        case HiveParser.TOK_TABREF => {

          // TODO: use TOK_WINDOW here once parser works.
          // Example for how it works right now:
          // FROM <tablename> window_<duration>
          val windowString = "window_"
          val aliasIndex = node.getChildren.size - 1
          val alias = node.getChild(aliasIndex).getText

          if (alias.contains(windowString)) {
            val startIndex = alias.lastIndexOf(windowString)
            val windowDurationStr = alias.substring(startIndex + windowString.length , alias.length)
            // Just use seconds for now
            val duration = Duration(windowDurationStr.toLong * 1000)
            val tableName =
              BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
            context.streamToWindow.put(tableName, duration)
          }
        }
        case _ => traverseChildren(node.getChildren)
      }
    }

    def traverseChildren(children: JavaList[Node]) {
      for (child <- children) {
        child match {
          case astNode: ASTNode => traverseNode(astNode)
          case _ => Unit
        }
      }
    }

    // TOK_FROM
    val fromTokenNode = queryTokenNode.getChild(0).asInstanceOf[ASTNode]
    traverseNode(fromTokenNode)
  }
}
