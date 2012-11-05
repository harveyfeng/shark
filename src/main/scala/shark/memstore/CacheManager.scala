package shark.memstore

import java.util.{ArrayList => JavaArrayList}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

import shark.SharkEnv
import spark.RDD
import spark.storage.BlockManagerCommunicator
import spark.storage.RDDInfo
import spark.storage.StorageLevel
import spark.Utils


class CacheManager {

  val keyToRdd = new collection.mutable.HashMap[CacheKey, RDD[_]]()

  val keyToStats = new collection.mutable.HashMap[CacheKey, collection.Map[Int, TableStats]]

  val blockManager = new BlockManagerCommunicator(SharkEnv.sc)

  def put(key: CacheKey, rdd: RDD[_], storageLevel: StorageLevel) {
    keyToRdd(key) = rdd
    rdd.persist(storageLevel)
  }

  def get(key: CacheKey): Option[RDD[_]] = keyToRdd.get(key)


  /**
   * Find all keys that are strings. Used to drop tables after exiting.
   */
  def getAllKeyStrings(): Set[String] = {
    keyToRdd.keys.map(_.key).collect { case k: String => k } toSet
  }

  /**
   * Return cache status of slaves.
   */
  def getCacheStatus: Map[String, (Long, Long)] = SharkEnv.sc.getSlavesMemoryStatus

  def getCacheStatus(output: JavaArrayList[String]) {
    val format = "%-26s %-17s %-15s"
    val header = String.format(format, "Host", "Capacity", "Used")
    for ((host, (maxMem, remainingMem)) <- getCacheStatus) {
      output.add(String.format(format, host, maxMem, (maxMem - remainingMem)))
    }
  }

  /**
   * Return RDD storage info.
   */
  def getStorageStatus(requestedRDDs: Set[String]): Seq[RDDInfo] =
    blockManager.getRDDStorageStatus(requestedRDDs)

  def getStorageStatus(output: JavaArrayList[String], requestedRDDs: Set[String]) {
    getCacheStatus(output)
    output.add("\n")

    val format = "%-10s %-15s %-17s %-20s %-20s %-15s"
    val header = String.format(format,
      "RDD ID", "Table", "Storage Level", "# RDD Partitions", "Size in Memory", "Size on Disk")
    output.add(header)
    for (rddInfo <- getStorageStatus(requestedRDDs)) {
      output.add(String.format(format,
        rddInfo.id, rddInfo.name, rddInfo.storageLevel, rddInfo.numPartitions,
          rddInfo.memSize, rddInfo.diskSize))
    }
  }
}