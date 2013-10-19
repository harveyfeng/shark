package shark.streaming

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import java.util.{HashMap => JavaHashMap, HashSet => JavaHashSet}

import org.apache.hadoop.io.{LongWritable, Text, Writable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, DStream, Duration, Time}
import org.apache.spark.util.MetadataCleaner

import shark.api.RDDTable
import shark.util.HiveUtils
import shark.SharkEnv
import shark.streaming.util.TwitterUtils
import shark.memstore2.CacheType

import twitter4j._


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
  
  private val _isNetworkInput = new JavaHashSet[String]();

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
      batchDuration: Duration): DStream[_] = {
    // val ssc = _durationToSsc.get(batchDuration) match {
    //   case ssc: StreamingContext => ssc
    //   case _ => createNewSsc(batchDuration)
    // }
    val ssc: StreamingContext = 
      if (_durationToSsc.size == 0) {
        createNewSsc(batchDuration)
      } else {
        _durationToSsc.values.toSeq(0)
      }
    // Note: only support new hadoop.mapreduce API. Hive uses the old one (package hadoop.mapred).
    // should throw an exception.
    val newStream = ssc.fileStream[LongWritable, Text, TextInputFormat](readDirectory).map(
      tup => new Text(tup._2))
    _streamToSsc.put(newStream, ssc)
    val streamName = name.toLowerCase
    _keyToDStream.put(streamName, newStream)
    _inputStreams.add(streamName)
    return newStream
  }
  
  def createSocketStream(name:String, batchDuration:Duration) : DStream[_] = {
    val ssc: StreamingContext = 
      if (_durationToSsc.size == 0) {
        createNewSsc(batchDuration)
      } else {
        _durationToSsc.values.toSeq(0)
      }
    val newStream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY_SER)
    //newStream.foreach(l => println(l.count()))
    
    val newTupleStream = newStream.map(line => {
    		val ls = line.split(",")
    		(ls(0).toInt, ls(1).toInt, ls(2).toInt, ls(3).toInt)
    	})
    	
    val manifests = RDDTable.getManifests(newTupleStream)
    SharkEnv.memoryMetadataManager.add(name, false, CacheType.HEAP)
    val colNames = Seq("a","b","c","d")
    
    HiveUtils.createTableInHive(name, colNames, manifests)
    
    val newSharkStream = newTupleStream.transform { rddOfTuples =>
        val table = RDDTable(rddOfTuples).saveAsDStreamTable(name, colNames)
        table
      }
    
    _isNetworkInput.add(name)

    // Force execute
    newSharkStream.foreach{rdd =>
      SharkEnv.memoryMetadataManager.put(name, rdd)
      println("SharkEnv.memoryMetadataManager.put(name, rdd)}")
      println("+++Name:" +  name)
      println("partitions: " + rdd.partitions.size)
      //println(rdd.collect().length)
    }

    _streamToSsc.put(newSharkStream, ssc)
    val streamName = name.toLowerCase
    _keyToDStream.put(streamName, newSharkStream)
    _inputStreams.add(streamName)

    return newSharkStream
  }

  def createTwitterStream(name: String, batchDuration: Duration): DStream[_] = {
    def safeValue(a: Any) = Option(a)
      .map(_.toString)
      .map(_.replace("\t", ""))
      .map(_.replace("\"", ""))
      .map(_.replace("\n", ""))
      .map(_.replaceAll("[\\p{C}]","")) // Control characters
      .getOrElse("")

    val hiveDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0")
    val fields: Seq[(Status => Any, String, String)] = Seq(
      (s => s.getId, "id", "BIGINT"),
      (s => s.getInReplyToStatusId, "reply_status_id", "BIGINT"),
      (s => s.getInReplyToUserId, "reply_user_id", "BIGINT"),
      (s => s.getRetweetCount, "retweet_count", "INT"),
      (s => s.getText, "text", "STRING"),
      (s => Option(s.getGeoLocation).map(_.getLatitude()).getOrElse(""), "latitude", "FLOAT"),
      (s => Option(s.getGeoLocation).map(_.getLongitude()).getOrElse(""), "longitude", "FLOAT"),
      (s => s.getSource, "source", "STRING"),
      (s => s.getUser.getId, "user_id", "INT"),
      (s => s.getUser.getName, "user_name", "STRING"),
      (s => s.getUser.getScreenName, "user_screen_name", "STRING"),
      (s => hiveDateFormat.format(s.getUser.getCreatedAt), "user_created_at", "TIMESTAMP"),
      (s => s.getUser.getFollowersCount, "user_followers", "BIGINT"),
      (s => s.getUser.getFavouritesCount, "user_favorites", "BIGINT"),
      (s => s.getUser.getLang, "user_language", "STRING"),
      (s => s.getUser.getLocation, "user_location", "STRING"),
      (s => s.getUser.getTimeZone, "user_timezone", "STRING"),
      (s => hiveDateFormat.format(s.getCreatedAt), "created_at", "TIMESTAMP")
    )

    val ssc = 
      if (_durationToSsc.size == 0) {
        createNewSsc(batchDuration)
      } else {
        _durationToSsc.values.toSeq(0)
      }
    TwitterUtils.configureTwitterCredentials()
    val newDStream = ssc.twitterStream(None, Nil, StorageLevel.MEMORY_ONLY_SER)
    // Transform this into a TableRDD
    val newTupleStream = newDStream.map(s =>
      // Tuple18
      (
        s.getId,
        s.getInReplyToStatusId,
        s.getInReplyToUserId,
        s.getRetweetCount,
        s.getText,
        Option(s.getGeoLocation).map(_.getLatitude()).getOrElse(0.0),
        Option(s.getGeoLocation).map(_.getLongitude()).getOrElse(0.0),
        s.getSource,
        s.getUser.getId,
        s.getUser.getName,
        s.getUser.getScreenName,
        hiveDateFormat.format(s.getUser.getCreatedAt),
        s.getUser.getFollowersCount,
        s.getUser.getFavouritesCount,
        s.getUser.getLang,
        s.getUser.getLocation,
        s.getUser.getTimeZone,
        hiveDateFormat.format(s.getCreatedAt)
      )
    )
    
    val manifests = RDDTable.getManifests(newTupleStream)
   
    SharkEnv.memoryMetadataManager.add(name, false, CacheType.HEAP)
    
    val colNames = fields.map{case (f, colName, hiveType) => colName}

    HiveUtils.createTableInHive(
      name,
      colNames,
      manifests)
    val newSharkStream = newTupleStream.transform { rddOfTuples =>
        RDDTable(rddOfTuples).saveAsDStreamTable(name, colNames)
      }
    
    _isNetworkInput.add(name)

    // Force execute
    newSharkStream.foreach{rdd =>
      {SharkEnv.memoryMetadataManager.put(name, rdd)
      //println("SharkEnv.memoryMetadataManager.put(name, rdd)}")
      //println("+++++++Name:" +  name)
      //println(rdd.collect().length)
      }
    }
    


    _streamToSsc.put(newSharkStream, ssc)
    val streamName = name.toLowerCase
    _keyToDStream.put(streamName, newSharkStream)
    _inputStreams.add(streamName)

    return newSharkStream
  }

  def isNetworkInput(name: String) = _isNetworkInput.contains(name)
  
  private def createNewSsc(batchDuration: Duration): StreamingContext = {
    // Set the default cleaner delay to an hour if not already set.
    // This should be sufficient for even 1 second interval.
    val newSsc = new StreamingContext(SharkEnv.sc, batchDuration)
    _durationToSsc.put(batchDuration, newSsc)
    return newSsc
  }
}
