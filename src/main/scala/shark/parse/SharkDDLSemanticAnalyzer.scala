package shark.parse

import java.util.{List => JavaList}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.TaskFactory
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, DDLSemanticAnalyzer, HiveParser}
import org.apache.hadoop.hive.ql.plan.{AlterTableDesc, DDLWork}

import org.apache.spark.rdd.{UnionRDD, RDD}

import shark.execution.SharkDDLWork
import shark.{LogHelper, SharkEnv}
import shark.memstore2.MemoryMetadataManager


class SharkDDLSemanticAnalyzer(conf: HiveConf) extends DDLSemanticAnalyzer(conf) with LogHelper {

  override def analyzeInternal(ast: ASTNode): Unit = {
    super.analyzeInternal(ast)

    ast.getToken.getType match {
      case HiveParser.TOK_DROPTABLE => {
        analyzeDropTableOrDropParts(ast)
      }
      case HiveParser.TOK_ALTERTABLE_DROPPARTS => {
        analyzeDropTableOrDropParts(ast)
      }
      case HiveParser.TOK_ALTERTABLE_ADDPARTS => {
        analyzeAlterTableAddParts(ast)
      }
      case HiveParser.TOK_ALTERTABLE_RENAME => {
        analyzeAlterTableRename(ast)
      }
      case HiveParser.TOK_ALTERTABLE_PROPERTIES => {
        analyzeAlterTableProps(ast)
      }
      case _ => Unit
    }
  }

  def analyzeDropTableOrDropParts(ast: ASTNode) {
    val tableName = getTableName(ast)
    // Create a SharkDDLTask only if the table is cached.
    if (SharkEnv.memoryMetadataManager.containsTable(tableName)) {
      // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with DDLTasks
      // and DDLWorks that contain DropTableDesc objects.
      for (ddlTask <- rootTasks) {
        val dropTableDesc = ddlTask.getWork.asInstanceOf[DDLWork].getDropTblDesc
        val sharkDDLWork = new SharkDDLWork(dropTableDesc)
        ddlTask.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
      }
    }
  }

  def analyzeAlterTableAddParts(ast: ASTNode) {
    val tableName = getTableName(ast)
    // Create a SharkDDLTask only if the table is cached.
    if (SharkEnv.memoryMetadataManager.containsTable(tableName)) {
      // Hive's DDLSemanticAnalyzer#analyzeInternal() will only populate rootTasks with DDLTasks
      // and DDLWorks that contain AddPartitionDesc objects.
      for (ddlTask <- rootTasks) {
        val addPartitionDesc = ddlTask.getWork.asInstanceOf[DDLWork].getAddPartitionDesc
        val sharkDDLWork = new SharkDDLWork(addPartitionDesc)
        ddlTask.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
      }
    }
  }

  private def analyzeAlterTableRename(astNode: ASTNode) {
    val oldTableName = getTableName(astNode)
    if (SharkEnv.memoryMetadataManager.containsTable(oldTableName)) {
      val alterTableDesc = getAlterTableDesc()
      val sharkDDLWork = new SharkDDLWork(alterTableDesc)
      ddlTask.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
    }
  }
  
  private def analyzeAlterTableProps(astNode: ASTNode) {
    val tableName = getTableName(astNode)
    if (SharkEnv.memoryMetadataManager.containsTable(tableName)) {
      val alterTableDesc = getAlterTableDesc()
      val sharkDDLWork = new SharkDDLWork(alterTableDesc)
      ddlTask.addDependentTask(TaskFactory.get(sharkDDLWork, conf))
    }
  }

  private def createSparkLoadTask(table: HiveTable) {
    
  }

  private def getAlterTableDesc(): AlterTableDesc = {
    // Hive's DDLSemanticAnalyzer#AnalyzeInternal() will only populate rootTasks with a DDLTask
    // and DDLWork that contains an AlterTableDesc.
    assert(hiveTasks.size == 1)
    val ddlTask = hiveTasks.head
    val ddlWork = ddlTask.getWork
    assert(ddlWork.isInstanceOf[DDLWork])

    return ddlWork.asInstanceOf[DDLWork].getAlterTblDesc
  }

  private def getTableName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node.getChild(0).asInstanceOf[ASTNode])
  }
}
