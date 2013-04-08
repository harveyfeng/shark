package shark.streaming

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import java.util.{HashMap => JavaHashMap, HashSet => JavaHashSet}

import shark.SharkEnv

import spark.storage.StorageLevel
import spark.streaming.{StreamingContext, DStream, Duration, Time}
import spark.util.MetadataCleaner

/*
 * Contains metadata for DStreams. Also, create and manage StreamingContexts here,
 * since each StreamingContext is associated with its own batch duration. This is
 * different from SparkContext, since we only keep one of those per Shark session.
 */
class StreamManager {

  // A collection of DStream sources for each table.
  private val _keyToDStream = new JavaHashMap[String, DStream[_]]
  private val _durationToSsc = new JavaHashMap[Duration, StreamingContext]()
  // TODO: this should only contain inputDStreams
  private val _streamToSsc = new JavaHashMap[DStream[_], StreamingContext]()
  private val _startedSscs = new JavaHashSet[StreamingContext]()

  // For testing/debugging
  private val _latestComputeTimes = new JavaHashMap[DStream[_], Time]()
  private val _commandContexts = new ArrayBuffer[StreamingCommandContext]()

  if (MetadataCleaner.getDelaySeconds < 0) {
    MetadataCleaner.setDelaySeconds(3600)
  }

  // TODO: make a getSsc() method that combs through dependencies.
  def hasSscStarted(stream: DStream[_]): Boolean = {
    var ssc = getSsc(stream)
    return _startedSscs.contains(ssc)
  }

  def addStartedSsc(ssc: StreamingContext) {
    _startedSscs.add(ssc)
  }

  def getSsc(stream: DStream[_]): StreamingContext = {
    var ssc = _streamToSsc.get(stream)
    var parent = stream.dependencies(0)
    // Note: we only use TransformedDStream, so all intermediate
    // DStreams have on dependency.
    while (ssc == null) {
      ssc = _streamToSsc.get(parent)
      parent = parent.dependencies(0)
    }

  	return ssc
  }

  def getSscs: Seq[StreamingContext] = _durationToSsc.values.toSeq

  def updateComputeTime(stream: DStream[_], time: Time) {
    _latestComputeTimes.put(stream, time)
  }

  def getLatestComputeTime(stream: DStream[_]): Time = {
    return _latestComputeTimes.get(stream)
  }

  def addCmdContext(ctx: StreamingCommandContext) = _commandContexts.append(ctx)

  def getStream(key: String): DStream[_] = _keyToDStream.get(key.toLowerCase)

  def getAllStreams(): Seq[String] = {
    _keyToDStream.keys.collect { case k: String => k } toSeq
  }

  def putIntermediateStream(name: String, stream: DStream[_], parent: DStream[_]) {
  	val ssc = _streamToSsc.get(parent)
  	_keyToDStream.put(name, stream)
  	_streamToSsc.put(stream, ssc)
  }

  def removeStream(key: String) {
  	val stream = _keyToDStream.get(key)
  	_streamToSsc.remove(stream)
  	_keyToDStream.remove(key)
    _latestComputeTimes.remove(stream)
  }

  def createTextFileStream(
      name: String,
      readDirectory: String,
      batchDuration: Duration,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    val ssc = _durationToSsc.get(batchDuration) match {
      case ssc: StreamingContext => ssc
      case _ => createNewSsc(batchDuration)
    }
    val newStream = ssc.textFileStream(readDirectory)
    _streamToSsc.put(newStream, ssc)
    _keyToDStream.put(name.toLowerCase, newStream)
    newStream.persist(storageLevel)
  }

  private def createNewSsc(batchDuration: Duration): StreamingContext = {
    // Set the default cleaner delay to an hour if not already set.
    // This should be sufficient for even 1 second interval.
    val newSsc = new StreamingContext(SharkEnv.sc, batchDuration)
    _durationToSsc.put(batchDuration, newSsc)
    return newSsc
  }
}
