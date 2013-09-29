package shark.streaming

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import java.util.{HashMap => JavaHashMap, HashSet => JavaHashSet}

import org.apache.hadoop.io.{LongWritable, Text, Writable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import shark.SharkEnv

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, DStream, Duration, Time}
import org.apache.spark.util.MetadataCleaner

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
  private val _inputStreams = new JavaHashSet[String]()
  private val _intermediateStreams = new JavaHashSet[String]()
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
    // Note: we only use TransformedDStream and WindowedDStream, so all intermediate
    // DStreams have one dependency.
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

  // TODO: better abstraction...TransformedDStreams are created in StreamingTask,
  // yet FileDStreams are created in StreamManager.
  def putIntermediateStream(key: String, stream: DStream[_], parent: DStream[_]) {
    val ssc = _streamToSsc.get(parent)
    val streamName = key.toLowerCase
    _keyToDStream.put(streamName, stream)
    _streamToSsc.put(stream, ssc)
    _intermediateStreams.add(streamName)
  }

  def isIntermediateStream(key: String) = _intermediateStreams.contains(key.toLowerCase)

  def isInputStream(key: String) = _inputStreams.contains(key.toLowerCase)

  def isStream(key: String) = _keyToDStream.contains(key.toLowerCase)

  def removeStream(key: String) {
    var streamName = key.toLowerCase
    val stream = _keyToDStream.get(streamName)
    _streamToSsc.remove(stream)
    _keyToDStream.remove(streamName)
    _latestComputeTimes.remove(stream)
    if (_inputStreams.contains(streamName)) _inputStreams.remove(streamName)
    if (_intermediateStreams.contains(streamName)) _intermediateStreams.remove(streamName)
  }

  // TODO: move to SharkStreamingContext?
  def createFileStream(
      name: String,
      readDirectory: String,
      batchDuration: Duration) {
    val ssc = _durationToSsc.get(batchDuration) match {
      case ssc: StreamingContext => ssc
      case _ => createNewSsc(batchDuration)
    }
    // Note: only support new hadoop.mapreduce API. Hive uses the old one (package hadoop.mapred).
    // should throw an exception.
    val newStream = ssc.fileStream[LongWritable, Text, TextInputFormat](readDirectory).map(tup => new Text(tup._2))
    _streamToSsc.put(newStream, ssc)
    val streamName = name.toLowerCase
    _keyToDStream.put(streamName, newStream)
    _inputStreams.add(streamName)
  }

  private def createNewSsc(batchDuration: Duration): StreamingContext = {
    // Set the default cleaner delay to an hour if not already set.
    // This should be sufficient for even 1 second interval.
    val newSsc = new StreamingContext(SharkEnv.sc, batchDuration)
    _durationToSsc.put(batchDuration, newSsc)
    return newSsc
  }
}
