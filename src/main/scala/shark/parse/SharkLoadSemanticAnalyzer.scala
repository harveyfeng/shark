/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.parse

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{CopyTask, MoveTask}
import org.apache.hadoop.hive.ql.parse.{ASTNode, BaseSemanticAnalyzer, LoadSemanticAnalyzer}
import org.apache.hadoop.hive.ql.plan._

import shark.execution.SparkLoadWork
import shark.LogHelper

class SharkLoadSemanticAnalyzer(hiveConf: HiveConf) extends LoadSemanticAnalyzer(hiveConf) {
  
  override def analyzeInternal(ast: ASTNode): Unit = {
    // Delegate to the LoadSemanticAnalyzer parent for error checking the source path formatting.
    super.analyzeInternal(ast)

    // Children of the AST root created for a LOAD DATA [LOCAL] INPATH ... statement are, in order:
    // 1. node containing the path specified by INPATH.
    // 2. internal TOK_TABNAME node that contains the table's name.
    // 3. (optional) node representing the LOCAL modifier.
    val tableASTNode = ast.getChild(1).asInstanceOf[ASTNode]

    // Get the source path that will be read. This can be either the path to a table's data
    // directory, or a subdirectory of a partitioned table.
    val tableSpec = new BaseSemanticAnalyzer.tableSpec(db, conf, tableASTNode)
    val hiveTable = tableSpec.tableHandle
    val partSpec = tableSpec.getPartSpec()
    val dataPath = if (partSpec == null) {
      // Non-partitioned table.
      hiveTable.getPath
    } else {
      // Partitioned table.
      val partition = db.getPartition(hiveTable, partSpec, false /* forceCreate */)
      //partition.getPartitionPath
      hiveTable.getPath
    }
    val isOverwrite = getLoadTableDesc().getReplace()

    // Capture a snapshot of the data directory being read. When executed, SparkLoadTask will
    // determine the input paths to read from a delta determined from this snapshot.
    val fs = dataPath.getFileSystem(hiveConf)
    val currentFiles = fs.listStatus(dataPath).map(_.getPath).toSet

    val newFileFilter = new PathFilter() {
      override def accept(path: Path) = currentFiles.contains(path)
    }

    // Create a SparkLoadTask that will use a HadoopRDD to read from the source directory. Set it
    // to be a dependent task of the LoadTask so that the SparkLoadTask is executed only if the Hive
    // task executes successfully.
    //val sparkLoadWork = new SparkLoadWork(tableDesc, dataPath.toString, Some(newFileFilter))
  }

  private def getLoadTableDesc(): LoadTableDesc = {
    assert(rootTasks.size == 1)

    // If the execution is local, a CopyTask will be the root task, with a MoveTask child.
    // Otherwise, a MoveTask will be the root.
    var rootTask = rootTasks.head
    val moveTask = if (rootTask.isInstanceOf[CopyTask]) {
      val firstChildTask = rootTask.getChildTasks.head
      assert(firstChildTask.isInstanceOf[MoveTask])
      firstChildTask
    } else {
      rootTask
    }

    // In Hive, LoadTableDesc is referred to as LoadTableWork...
    moveTask.getWork.asInstanceOf[MoveWork].getLoadTableWork
  }
}
