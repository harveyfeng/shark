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

package shark.execution

import scala.collection.JavaConversions
import scala.collection.mutable.{ArrayBuffer, Map}

import com.google.common.collect.{Ordering => GOrdering}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.HiveInputFormat
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}

import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner}
import org.apache.spark.rdd.{RDD, ShuffledRDD, UnionRDD}
import org.apache.spark.storage.StorageLevel

import shark.memstore2.{TablePartition, TablePartitionBuilder, TablePartitionStats}
import shark.{SharkEnv, Utils}



/**
 * A set of RDD-related functions that provide some handy features in addition
 * to Spark's built-in abstractions.
 */
object RDDUtils {

  def getStorageLevelOfRDD(rdd: RDD[_]): StorageLevel = {
    rdd match {
      case u: UnionRDD[_] => {
        // Find the storage level of a UnionRDD from the storage levels of RDDs that compose it.
        // A StorageLevel.NONE is returned if all of those RDDs have StorageLevel.NONE.
        // Mutually recursive if any RDD in 'u.rdds' is a UnionRDD.
        getStorageLevelOfRDDs(u.rdds)
      }
      case _ => rdd.getStorageLevel
    }
  }

  /**
   * Returns the storage level of a sequence of RDDs, interpreted as the storage level of the first
   * RDD in the sequence that persisted in memory or disk.
   *
   * @param rdds The sequence of RDDs to find the StorageLevel of.
   */
  def getStorageLevelOfRDDs(rdds: Seq[RDD[_]]): StorageLevel = {
    rdds.foldLeft(StorageLevel.NONE) {
      (s, r) => {
        if (s == StorageLevel.NONE) {
          // Mutally recursive if 'r' is a UnionRDD.
          getStorageLevelOfRDD(r)
        } else {
          // Some RDD in 'rdds' is persisted in memory or disk, so return early.
          return s
        }
      }
    }
  }

  def unpersistRDD(rdd: RDD[_]): RDD[_] = {
    rdd match {
      case u: UnionRDD[_] => {
        // Usually, a UnionRDD will not be persisted to avoid data duplication.
        u.unpersist()
        // unpersist() all parent RDDs that compose the UnionRDD. Don't propagate past the parents,
        // since a grandparent of the UnionRDD might have multiple child RDDs (i.e., the sibling of
        // the UnionRDD's parent is persisted in memory).
        u.rdds.map {
          r => r.unpersist()
        }
      }
      case r => r.unpersist()
    }
    return rdd
  }

  def transformToTableRDD(
      rdd: RDD[_],
      generateOIFunc: () => StructObjectInspector,
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
    ): (RDD[TablePartition], ArrayBuffer[(Int, TablePartitionStats)]) = {

    // Use an Accumulator to collect statistics
    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())

    // Create the RDD object.
    val transformedRDD = rdd.mapPartitionsWithIndex { case(partitionIndex, partitionIter) =>
      //val ois = manifests.map(HiveUtils.getJavaPrimitiveObjectInspector)
      val ois = generateOIFunc()
      val builder = new TablePartitionBuilder(ois, 1000000, shouldCompress = false)

      for (product <- partitionIter) {
        builder.incrementRowCount()
        // TODO: this is not the most efficient code to do the insertion ...
        product.productIterator.zipWithIndex.foreach { case (v, i) =>
          builder.append(i, v.asInstanceOf[Object], ois(i))
        }
      }

      statsAcc += Tuple2(partitionIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
      Iterator(builder.build())
    }.persist(storageLevel)

    // Force evaluate to put the data in memory. This may throw an exception.
    transformedRDD.context.runJob(
      rdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))

    return (transformedRDD, statsAcc.value)
  }

  def unionStatsMaps(
      targetStatsMap: ArrayBuffer[(Int, TablePartitionStats)],
      otherStatsMap: ArrayBuffer[(Int, TablePartitionStats)]
    ): ArrayBuffer[(Int, TablePartitionStats)] = {
    val targetStatsMapSize = targetStatsMap.size
    for ((otherIndex, tableStats) <- otherStatsMap) {
      targetStatsMap.append((otherIndex + targetStatsMapSize, tableStats))
    }
    return otherStatsMap
  }

  def createHadoopRdd(
      path: String,
      ifc: Class[InputFormat[Writable, Writable]],
      tableDesc: TableDesc,
      hiveConf: HiveConf
    ): RDD[Writable] = {
    val conf = new JobConf(hiveConf)
    if (tableDesc != null) {
      Utilities.copyTableJobPropertiesToConf(tableDesc, conf)
    }
    FileInputFormat.setInputPaths(conf, path)
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    conf.set("io.file.buffer.size", bufferSize)

    // Set s3/s3n credentials. Setting them in conf ensures the settings propagate
    // from Spark's master all the way to Spark's slaves.
    var s3varsSet = false
    val s3vars = Seq("fs.s3n.awsAccessKeyId", "fs.s3n.awsSecretAccessKey",
      "fs.s3.awsAccessKeyId", "fs.s3.awsSecretAccessKey").foreach { variableName =>
      if (hiveConf.get(variableName) != null) {
        s3varsSet = true
        conf.set(variableName, hiveConf.get(variableName))
      }
    }

    // If none of the s3 credentials are set in Hive conf, try use the environmental
    // variables for credentials.
    if (!s3varsSet) {
      Utils.setAwsCredentials(conf)
    }

    // Choose the minimum number of splits. If mapred.map.tasks is set, use that unless
    // it is smaller than what Spark suggests.
    val minSplits = math.max(hiveConf.getInt("mapred.map.tasks", 1), SharkEnv.sc.defaultMinSplits)
    val rdd = SharkEnv.sc.hadoopRDD(conf, ifc, classOf[Writable], classOf[Writable], minSplits)

    // Only take the value (skip the key) because Hive works only with values.
    return rdd.map(_._2)
  }

  /**
   * Repartition an RDD using the given partitioner. This is similar to Spark's partitionBy,
   * except we use the Shark shuffle serializer.
   */
  def repartition[K: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)], part: Partitioner)
    : RDD[(K, V)] =
  {
    new ShuffledRDD[K, V, (K, V)](rdd, part).setSerializer(SharkEnv.shuffleSerializerName)
  }

  /**
   * Sort the RDD by key. This is similar to Spark's sortByKey, except that we use
   * the Shark shuffle serializer.
   */
  def sortByKey[K <: Comparable[K]: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)])
    : RDD[(K, V)] =
  {
    val part = new RangePartitioner(rdd.partitions.length, rdd)
    val shuffled = new ShuffledRDD[K, V, (K, V)](rdd, part)
      .setSerializer(SharkEnv.shuffleSerializerName)
    shuffled.mapPartitions(iter => {
      val buf = iter.toArray
      buf.sortWith((x, y) => x._1.compareTo(y._1) < 0).iterator
    }, true)
  }

  /**
   * Return an RDD containing the top K (K smallest key) from the given RDD.
   */
  def topK[K <: Comparable[K]: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)], k: Int)
    : RDD[(K, V)] =
  {
    // First take top K on each partition.
    val partialSortedRdd = partitionTopK(rdd, k)
    // Then merge all partitions into a single one, and take the top K on that partition.
    partitionTopK(repartition(partialSortedRdd, new HashPartitioner(1)), k)
  }

  /**
   * Take top K on each partition and return a new RDD.
   */
  def partitionTopK[K <: Comparable[K]: ClassManifest, V: ClassManifest](
    rdd: RDD[(K, V)], k: Int): RDD[(K, V)] = {
    rdd.mapPartitions(iter => topK(iter, k))
  }

  /**
   * Return top K elements out of an iterator.
   */
  private def topK[K <: Comparable[K]: ClassManifest, V: ClassManifest](
    it: Iterator[(K, V)], k: Int): Iterator[(K, V)]  = {
    val ordering = new GOrdering[(K,V)] {
      override def compare(l: (K, V), r: (K, V)) = {
        (l._1).compareTo(r._1)
      }
    }
    // Guava only takes Java iterators. Convert the iterator into Java iterator and then
    // convert it back to Scala.
    JavaConversions.asScalaIterator(
      ordering.leastOf(JavaConversions.asJavaIterator(it), k).iterator)
  }

}
