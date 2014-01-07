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

package shark.tachyon

import java.nio.ByteBuffer
import java.util.BitSet

import org.apache.spark.rdd.RDD

import shark.memstore2.{Table, TablePartition}


class TachyonUtilImpl(val master: String, val warehousePath: String) extends TachyonUtil {

  override def pushDownColumnPruning(rdd: RDD[_], columnUsed: BitSet): Boolean = false

  override def tachyonEnabled(): Boolean = false

  override def tableExists(tableKey: String, hivePartitionKey: Option[String]): Boolean = {
    throw new UnsupportedOperationException(
      "This version of Shark is not compiled with Tachyon support.")
  }

  override def dropTable(tableKey: String, hivePartitionKey: Option[String]): Boolean = {
    throw new UnsupportedOperationException(
      "This version of Shark is not compiled with Tachyon support.")
  }

  override def getTableMetadata(tableKey: String, hivePartitionKey: Option[String]): ByteBuffer = {
    throw new UnsupportedOperationException(
      "This version of Shark is not compiled with Tachyon support.")
  }

  override def createRDD(tableKey: String, hivePartitionKey: Option[String]): RDD[TablePartition] = {
    throw new UnsupportedOperationException(
      "This version of Shark is not compiled with Tachyon support.")
  }

  override def createTableWriter(
      tableKey: String,
      hivePartitionKey: Option[String],
      numColumns: Int): TachyonTableWriter = {
    throw new UnsupportedOperationException(
      "This version of Shark is not compiled with Tachyon support.")
  }
}
