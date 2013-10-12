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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.plan._ 

class SharkLoadSemanticAnalyzer(conf: HiveConf) extends LoadSemanticAnalyzer {
  
  override def analyzeInternal(ast: ASTNode): Unit = {
    // Delage to the LoadSemanticAnalyzer parent for error checking the source path and path name
    // formatting 
    super.analyzeInternal(ast)

    val tableASTRoot = ast.getChild(1).asInstanceOf[ASTNode]
    tableSpec analyzerTableSpec = new tableDesc(conf, db, tableASTRoot)

    // Get the source path that will be read.
    val loadTableDesc = findLoadTableDesc()
    val sourceDirectory = loadTableDesc.getSourcedir()

    // Create a SparkLoadTask that will use a HadoopRDD to read from the source directory. Set it
    // to be a dependent task of the LoadTask so that the SparkLoadTask is executed only if the Hive
    // task executes successfully.
    val sparkLoadWork = new SparkLoadWork(sourceDirectory, analyzeTableSpec.tableHandle)
  }

  private def findLoadTableDesc(): LoadTableDesc = {
    assert(rootTasks.size == 1)

    // If the execution is local, a CopyTask will be the root task, with a MoveTask child.
    // Otherwise, a MoveTask will be the root.
    var rootTask = rootTasks.head
    val moveTask =
      if (rootTask.isInstanceOf[CopyTask]) {
        val firstChildTask = rootTask.getChildTasks.head
        assert(firstChildTask.isInstanceOf[MoveTask])
      } else {
        rootTask
      }

    // Note that in Hive, LoadTableDesc is referred to as LoadTableWork :-(
    return moveTask.getWork.asInstanceOf[MoveWork].getLoadTableWork
  }
}
