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

package shark.api

import scala.collection.mutable.ArrayBuffer

import shark.SharkEnv
import shark.memstore2.{CacheType, TablePartitionStats, TablePartition, TablePartitionBuilder}
import shark.util.HiveUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.DStream


class RDDTableFunctions(self: RDD[Product], manifests: Seq[ClassManifest[_]]) {

  def saveAsTable(tableName: String, fields: Seq[String]): Boolean = {
    require(fields.size == this.manifests.size,
      "Number of column names != number of fields in the RDD.")

    // Get a local copy of the manifests so we don't need to serialize this object.
    val manifests = this.manifests

    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())

    // Create the RDD object.
    val rdd = self.mapPartitionsWithIndex { case(partitionIndex, iter) =>
      val ois = manifests.map(HiveUtils.getJavaPrimitiveObjectInspector)
      val builder = new TablePartitionBuilder(ois, 1000000, shouldCompress = false)

      for (p <- iter) {
        builder.incrementRowCount()
        // TODO: this is not the most efficient code to do the insertion ...
        p.productIterator.zipWithIndex.foreach { case (v, i) =>
          builder.append(i, v.asInstanceOf[Object], ois(i))
        }
      }

      statsAcc += Tuple2(partitionIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
      Iterator(builder.build())
    }.persist()

    var isSucessfulCreateTable = HiveUtils.createTableInHive(tableName, fields, manifests)

    // Put the table in the metastore. Only proceed if the DDL statement is executed successfully.
    if (isSucessfulCreateTable) {
      // Force evaluate to put the data in memory.
      SharkEnv.memoryMetadataManager.put(tableName, rdd)
      try {
        rdd.context.runJob(rdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
      } catch {
        case _ => {
          // Intercept the exception thrown by SparkContext#runJob() and handle it silently. The
          // exception message should already be printed to the console by DDLTask#execute().
          HiveUtils.dropTableInHive(tableName)
          // Drop the table entry from MemoryMetadataManager.
          SharkEnv.unpersist(tableName)
          isSucessfulCreateTable = false
        }
      }

      // Gather the partition statistics.
      SharkEnv.memoryMetadataManager.putStats(tableName, statsAcc.value.toMap)
    }
    return isSucessfulCreateTable
  }

  // Only creates the "table" once.
  def saveAsDStreamTable(streamName: String, fields: Seq[String]): RDD[TablePartition] = {
    require(fields.size == this.manifests.size,
      "Number of column names != number of fields in the RDD.")

    // Get a local copy of the manifests so we don't need to serialize this object.
    val manifests = this.manifests

    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())

    // Create the RDD object.
    val rdd = self.mapPartitionsWithIndex { case(partitionIndex, iter) =>
      val ois = manifests.map(HiveUtils.getJavaPrimitiveObjectInspector)
      val builder = new TablePartitionBuilder(ois, 1000000, shouldCompress = false)

      for (p <- iter) {
        builder.incrementRowCount()
        // TODO: this is not the most efficient code to do the insertion ...
        p.productIterator.zipWithIndex.foreach { case (v, i) =>
          builder.append(i, v.asInstanceOf[Object], ois(i))
        }
      }

      statsAcc += Tuple2(partitionIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
      Iterator(builder.build())
    }.persist(StorageLevel.MEMORY_ONLY_SER)
    var isSucessfulCreateTable = false
    RDDTable.synchronized {
      if (SharkEnv.memoryMetadataManager.contains(streamName)) {
        isSucessfulCreateTable = true
      } else {
        //isSucessfulCreateTable = HiveUtils.createTableInHive(streamName, fields, manifests)
        isSucessfulCreateTable = true

        // Put the table in the metastore. Only proceed if the DDL statement is executed successfully.
        // if (isSucessfulCreateTable) {
          // Force evaluate to put the data in memory.
        //  try {
         //   rdd.context.runJob(rdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
        //  } catch {
          //  case _ => {
              // Intercept the exception thrown by SparkContext#runJob() and handle it silently. The
              // exception message should already be printed to the console by DDLTask#execute().
        //      HiveUtils.dropTableInHive(streamName)
        //      // Drop the table entry from MemoryMetadataManager.
        //      SharkEnv.unpersist(streamName)
        //      isSucessfulCreateTable = false
        //    }
        //  }

          // Gather the partition statistics.
          SharkEnv.memoryMetadataManager.putStats(streamName, statsAcc.value.toMap)
        //}
      }
    }
    return rdd
  }
}


object RDDTable {
  val lock = new Object()
  private type M[T] = ClassManifest[T]
  private def m[T](implicit m : ClassManifest[T]) = classManifest[T](m)


  // We need 19 for the Twitter API
  def getManifests[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M,
            T10: M, T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M]
    (
      dStream: DStream[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18)]
    ) = {
    Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10], m[T11],
        m[T12], m[T13], m[T14], m[T15], m[T16], m[T17], m[T18])
  }
  
    def getManifests[T1: M, T2: M, T3: M, T4: M]
    (
      dStream: DStream[(T1, T2, T3, T4)]
    ) = {
    Seq(m[T1], m[T2], m[T3], m[T4])
  }

  // We need 19 for the Twitter API
  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M,
            T10: M, T11: M, T12: M, T13: M, T14: M, T15: M, T16: M, T17: M, T18: M]
    (
      rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18)]
    ) = {
    new RDDTableFunctions(rdd.asInstanceOf[RDD[Product]],
      Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10], m[T11],
          m[T12], m[T13], m[T14], m[T15], m[T16], m[T17], m[T18]))
  }

  def apply[T1: M, T2: M](rdd: RDD[(T1, T2)]) = {
    new RDDTableFunctions(rdd.asInstanceOf[RDD[Product]], Seq(m[T1], m[T2]))
  }

  def apply[T1: M, T2: M, T3: M](rdd: RDD[(T1, T2, T3)]) = {
    new RDDTableFunctions(rdd.asInstanceOf[RDD[Product]], Seq(m[T1], m[T2], m[T3]))
  }

  def apply[T1: M, T2: M, T3: M, T4: M](rdd: RDD[(T1, T2, T3, T4)]) = {
    new RDDTableFunctions(rdd.asInstanceOf[RDD[Product]], Seq(m[T1], m[T2], m[T3], m[T4]))
  }

  def apply[T1: M, T2: M, T3: M, T4: M, T5: M](rdd: RDD[(T1, T2, T3, T4, T5)]) = {
    new RDDTableFunctions(rdd.asInstanceOf[RDD[Product]], Seq(m[T1], m[T2], m[T3], m[T4], m[T5]))
  }

  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M](rdd: RDD[(T1, T2, T3, T4, T5, T6)]) = {
    new RDDTableFunctions(rdd.asInstanceOf[RDD[Product]],
      Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6]))
  }

  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M](
      rdd: RDD[(T1, T2, T3, T4, T5, T6, T7)]) = {
    new RDDTableFunctions(rdd.asInstanceOf[RDD[Product]],
      Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7]))
  }

  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M](
      rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8)]) = {
    new RDDTableFunctions(rdd.asInstanceOf[RDD[Product]],
      Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8]))
  }

  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M](
      rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9)]) = {
    new RDDTableFunctions(rdd.asInstanceOf[RDD[Product]],
      Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9]))
  }

  def apply[T1: M, T2: M, T3: M, T4: M, T5: M, T6: M, T7: M, T8: M, T9: M, T10: M](
      rdd: RDD[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)]) = {
    new RDDTableFunctions(rdd.asInstanceOf[RDD[Product]],
      Seq(m[T1], m[T2], m[T3], m[T4], m[T5], m[T6], m[T7], m[T8], m[T9], m[T10]))
  }
}
