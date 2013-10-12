/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.DriverContext
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.hadoop.hive.ql.exec.{Task => HiveTask}
import org.apache.hadoop.io.Writable


private[shark]
class SparkLoadWork(path: String, targetTable: HiveTable) extends java.io.Serializable

private[shark]
class SparkLoadTask extends HiveTask[SparkLoadWork] with Serializable with LogHelper {
  
  override def execute(driveContext: DriverContext): Int = {
    logDebug("Executing " + this.getClass.getname)
    val tableName = work.targetTable.getTableName
    val ifc = work.targetTable.getInputFormatClass
      .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]

    val hadoopRDD = RDDUtils.createHadoopRdd(path, ifc, conf)
    val getOIsFunc() = {
      val deserializer = work.targetTable.getDeserializer
      val rowObjectInspector = deserializer.getObjectInspector()
      rowObjectinspector.getAllStructFieldRefs.map(_.getFieldObjectInspector)
    }

    var (newTableRDD, newStatsMap) = RDDUtils.transformToTableRDD(hadoopRDD, getOIsFunc)

    // LOAD DATA INPATH behaves like an INSERT INTO, so use a UnionRDD if there a previous RDD
    // entry for the target table.
    SharkEnv.memoryMetadataManager.get(tableName) match {
      case Some(definedRDD) => {
        transformedRDD.union(transformedRDD.context, definedRDD)
        val oldStatsMapOpt = SharkEnv.memoryMetadataManager.getStats(tableName)
        assert(oldStatsMapOpt.isDefined)
        RDDUtils.unionStatsMaps(newStatsMap, oldStatsMapOpt.get)
      }
      case None => {
        // Create a new entry in the Shark metastore.
        SharkEnv.memoryMetadataManager.addTable(tableName, transformedRDD)
        transformedRDD
      }
    }
    SharkEnv.memoryMetadataManager.put(tableName, newTableRDD)
    SharkEnv.memoryMetadataManager.putStats(tableName, newStatsMap)
  }
}
