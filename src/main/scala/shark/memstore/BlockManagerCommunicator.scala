package spark.storage

import java.util.{ArrayList => JavaArrayList}
import scala.collection.mutable.ArrayBuffer

import spark.{SparkContext, SparkEnv}

case class RDDInfo(
    id: Int,
    name: String,
    storageLevel: StorageLevel,
    numPartitions: Int,
    memSize: Long,
    diskSize: Long)

class BlockManagerCommunicator(sc: SparkContext) {

  val blockManager = SparkEnv.get.blockManager

  def getRDDStorageStatus(requestedRDDs: Set[String]): Seq[RDDInfo] = {
    val rddInfos = new ArrayBuffer[RDDInfo]
    val storageStatuses = blockManager.master.askMaster(GetStorageStatus)
      .asInstanceOf[ArrayBuffer[StorageStatus]]

    // Filter out non-rdds
    storageStatuses.flatMap(_.blocks).filter { case(blockId, blockStatuses) =>
      blockId.startsWith("rdd")
    }.groupBy { case (k, v) =>
      k.substring(0, k.lastIndexOf('_'))
    }.foreach { case (k, v) =>
      // Get table name from each rddId
      val rddId = k.split('_').last.toInt
      val rddName: String = Option(sc.rddNames.get(rddId)).getOrElse(k)
      if (requestedRDDs.contains(rddName)) {
        val rddStorageLevel = sc.persistentRdds.get(rddId).getStorageLevel
        val blockStatuses = v.map(_._2).toArray
        // Add up memory and disk sizes
        val (totalMemSize, totalDiskSize) = blockStatuses.map { x =>
          (x.memSize, x.diskSize) }.reduce { (x, y) => (x._1 + y._1, x._2 + y._2) }
        rddInfos += RDDInfo(
          rddId,
          rddName,
          rddStorageLevel,
          blockStatuses.length,
          totalMemSize,
          totalDiskSize)
      }
    }
    return rddInfos
  }
}
