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

package shark.memstore2

import java.util.{HashMap => JavaHashMap}

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.hadoop.hive.ql.metadata.Hive

import shark.{LogHelper, SharkEnv}
import shark.util.QueryRewriteUtils

/**
 * Singleton used to reload RDDs upon server restarts.
 */
object TableRecovery extends LogHelper {

  val db = Hive.get()

  /**
   * Loads any cached tables with MEMORY as its `shark.cache` property.
   * @param cmdRunner The runner that is responsible for taking a cached table query and
   *        a) Creating the table metadata in Hive Meta Store
   *        b) Loading the table as an RDD in memory
   *        @see SharkServer for an example usage.
   */
  def reloadRdds(cmdRunner: String => Unit) {
    // Filter for tables that should be reloaded into the cache.
    val currentDbName = db.getCurrentDatabase()
    for (databaseName <- db.getAllDatabases(); tableName <- db.getAllTables(databaseName)) {
      val hiveTable = db.getTable(databaseName, tableName)
      val tblProps = hiveTable.getParameters
      val cacheMode = CacheType.fromString(tblProps.get(SharkTblProperties.CACHE_FLAG.varname))
      if (cacheMode == CacheType.MEMORY) {
        logInfo("Reloading %s.%s into memory.".format(databaseName, tableName))
        val cmd = QueryRewriteUtils.cacheToAlterTable("CACHE %s".format(tableName))
        cmdRunner(cmd)
      } else if (cacheMode == CacheType.TACHYON) {
        // Persistece and write-though cache for Tachyon tables are managed by the Tachyon master.
        // So, Shark just needs to create a Shark Table entry in the MemoryMetadataManager.
        if (hiveTable.isPartitioned()) {
          // Create a PartitionedMemoryTable entry and add all partition keys.
          val partitionedMemoryTable = SharkEnv.memoryMetadataManager.createPartitionedMemoryTable(
            databaseName, tableName, cacheMode, tblProps)
          val columnNames = hiveTable.getPartCols.map(_.getName)
          db.getPartitions(hiveTable).map { hivePartition =>
            val partSpec = new JavaHashMap[String, String]()
            val values = hivePartition.getValues()
            columnNames.zipWithIndex.map { case(name, index) => partSpec.put(name, values(index)) }
            val hivePartitionKey = MemoryMetadataManager.makeHivePartitionKeyStr(
              columnNames, partSpec)
            // Tachyon RDDs are constructed during table scans. Add a NULL `outputRDD` placeholder
            // so that partition cache policies still work.
            partitionedMemoryTable.putPartition(hivePartitionKey, newRDD = null)
          }
        } else {
          // Create a MemoryTable entry.
          SharkEnv.memoryMetadataManager.createMemoryTable(databaseName, tableName, cacheMode)
        }
      }
    }
    db.setCurrentDatabase(currentDbName)
  }
}
