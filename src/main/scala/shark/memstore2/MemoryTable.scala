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

import org.apache.spark.rdd.RDD

import scala.collection.mutable.Buffer


/**
 * A metadata container for a table in Shark that's backed by an RDD.
 */
private[shark] class MemoryTable(
    databaseName: String,
    tableName: String,
    cacheMode: CacheType.CacheType)
  extends Table(databaseName, tableName, cacheMode) {

  var rddValueOpt: Option[RDDValue] = _

  def put(
  	  newRDD: RDD[TablePartition],
  	  newStats: collection.Map[Int, TablePartitionStats]
  	): Option[(RDD[TablePartitionStats, collection.Map[Int, TablePartitionStats]])] = {
  	val prevRDDAndStatsOpt = rddValueOpt.map(_.toTuple)
  	if (rddValueOpt.isDefined) {
  	  rddValueOpt.foreach { rddValue =>
  	  	rddValue.rdd = newRDD
  	  	rddValue.stats = newStats
  	  }
  	} else {
      rddValueOpt = Some(new RDDValue(newRDD, newStats))
  	}
    prevRDDAndStatsOpt 
  }

  def update(
  	  newRDD: RDD[TablePartition],
  	  newStats: Buffer[Int, TablePartitionStats]
  	): Option[(RDD[TablePartitionStats, collection.Map[Int, TablePartitionStats]])] = {
    val prevRDDAndStatsOpt = rddValueOpt.map(_.toTuple)
  	if (rddValueOpt.isDefined) {
      val (prevRDD, prevStats) = (prevRDDAndStatsOpt.get._1, prevRDDAndStatsOpt.get._1)
      val updatedRDDValue = rddValueOpt.get
      updatedRDDValue.rdd = RDDUtils.unionAndFlatten(prevRDD, newRDD)
      updatedRDDValue.stats = Table.mergeStats(stats, prevStats)
    } else {
      put(newRDD, newStats)
    }
    prevRDDAndStatsOpt
  }

  def getTable = rddValueOpt.map(_.rdd)

  def getStats = rddValueOpt.map(_.stats)

}
