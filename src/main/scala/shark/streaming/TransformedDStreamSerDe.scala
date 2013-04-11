package shark.streaming

import java.util.{ArrayList => JavaArrayList, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.`lazy`.{LazyFactory, LazySimpleSerDe}
import org.apache.hadoop.hive.serde2.{SerDe, SerDeStats}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, 
  StructField, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo
import org.apache.hadoop.io.Writable


import shark.{LogHelper, SharkEnvSlave}

class TransformedDStreamSerDe extends SerDe with LogHelper {

  var objectInspector: StructObjectInspector = _

  override def initialize(conf: Configuration, tbl: Properties) {
    val serDeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, this.getClass.getName)

    val columnNames = serDeParams.getColumnNames()
    val columnTypes = serDeParams.getColumnTypes()
    val fieldOIs = new JavaArrayList[ObjectInspector]()
    for (i <- 0 until columnNames.size) {
      val typeInfo = columnTypes.get(i)
      val fieldOI = SharkEnvSlave.objectInspectorLock.synchronized {
          LazyFactory.createLazyObjectInspector(
            typeInfo, serDeParams.getSeparators(), 1, serDeParams.getNullSequence(),
            serDeParams.isEscaped(), serDeParams.getEscapeChar())
        }
      fieldOIs.add(fieldOI)
    }

    objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      columnNames, fieldOIs)
  }

  override def deserialize(blob: Writable): Object =
      throw new UnsupportedOperationException("TransformedDStreamSerDe.deserialize()")

  override def serialize(obj: Object, objInspector: ObjectInspector): Writable =
      throw new UnsupportedOperationException("TransformedDStreamSerDe.serialize()")
  
  override def getSerDeStats: SerDeStats = {
    // TODO: Stats are not collected yet.
    new SerDeStats
  }

  override def getObjectInspector: ObjectInspector = objectInspector

  override def getSerializedClass: Class[_ <: Writable] = classOf[Writable]
}
