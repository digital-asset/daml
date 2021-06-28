// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v1.ValueOuterClass
import org.checkerframework.checker.nullness.qual.NonNull
import java.util._

object DamlRecord {
  private def fieldsListToHashMap(@NonNull fields: util.List[DamlRecord.Field]) = if (
    fields.isEmpty || !fields.get(0).getLabel.isPresent
  ) Collections.emptyMap
  else {
    val fieldsMap = new util.HashMap[String, Value](fields.size)
    import scala.collection.JavaConversions._
    for (field <- fields) { fieldsMap.put(field.getLabel.get, field.getValue) }
    fieldsMap
  }
  @NonNull def fromProto(record: ValueOuterClass.Record) = {
    val fields = new util.ArrayList[DamlRecord.Field](record.getFieldsCount)
    val fieldsMap = new util.HashMap[String, Value](record.getFieldsCount)
    import scala.collection.JavaConversions._
    for (recordField <- record.getFieldsList) {
      val field = Field.fromProto(recordField)
      fields.add(field)
      if (field.getLabel.isPresent) fieldsMap.put(field.getLabel.get, field.getValue)
    }
    if (record.hasRecordId) {
      val recordId = Identifier.fromProto(record.getRecordId)
      new DamlRecord(Optional.of(recordId), fields, fieldsMap)
    } else new DamlRecord(Optional.empty, fields, fieldsMap)
  }
  object Field {
    def fromProto(field: ValueOuterClass.RecordField) = {
      val label = field.getLabel
      val value = Value.fromProto(field.getValue)
      if (label.isEmpty) new DamlRecord.Field(value)
      else new DamlRecord.Field(label, value)
    }
  }
  class Field {
    final private var label = null
    final private var value = null
    def this(@NonNull label: String, @NonNull value: Value) {
      this()
      this.label = Optional.of(label)
      this.value = value
    }
    def this(@NonNull value: Value) {
      this()
      this.label = Optional.empty
      this.value = value
    }
    @NonNull def getLabel = label
    @NonNull def getValue = value
    def toProto = {
      val builder = ValueOuterClass.RecordField.newBuilder
      this.label.ifPresent(builder.setLabel)
      builder.setValue(this.value.toProto)
      builder.build
    }
    override def equals(o: Any): Boolean = {
      if (this eq o) return true
      if (o == null || (getClass ne o.getClass)) return false
      val field = o.asInstanceOf[DamlRecord.Field]
      Objects.equals(label, field.label) && Objects.equals(value, field.value)
    }
    override def hashCode = Objects.hash(label, value)
    override def toString = "Field{" + "label=" + label + ", value=" + value + '}'
  }
}
class DamlRecord(
    val recordId: Optional[Identifier],
    val fields: List[DamlRecord.Field],
    val fieldsMap: Map[String, Value],
) extends Value {
  def this(@NonNull recordId: Identifier, @NonNull fields: DamlRecord.Field*) {
    this(recordId, util.Arrays.asList(fields))
  }
  def this(@NonNull fields: DamlRecord.Field*) { this(util.Arrays.asList(fields)) }
  def this(@NonNull recordId: Identifier, @NonNull fields: util.List[DamlRecord.Field]) {
    this(Optional.of(recordId), fields, DamlRecord.fieldsListToHashMap(fields))
  }
  def this(@NonNull fields: util.List[DamlRecord.Field]) {
    this(Optional.empty, fields, DamlRecord.fieldsListToHashMap(fields))
  }
  override def toProto = ValueOuterClass.Value.newBuilder.setRecord(this.toProtoRecord).build
  def toProtoRecord = {
    val recordBuilder = ValueOuterClass.Record.newBuilder
    this.recordId.ifPresent((recordId: Identifier) => recordBuilder.setRecordId(recordId.toProto))
    import scala.collection.JavaConversions._
    for (field <- this.fields) { recordBuilder.addFields(field.toProto) }
    recordBuilder.build
  }
  @NonNull def getRecordId = recordId
  @NonNull def getFields = fields

  /** @return the Map of this DamlRecord fields containing the records that have the label
    */
  @NonNull def getFieldsMap = fieldsMap
  override def toString = "DamlRecord{" + "recordId=" + recordId + ", fields=" + fields + '}'
  override def equals(o: Any): Boolean = {
    if (this eq o) return true
    if (o == null || (getClass ne o.getClass)) return false
    val record = o.asInstanceOf[DamlRecord]
    Objects.equals(recordId, record.recordId) && Objects.equals(fields, record.fields)
  }
  override def hashCode = Objects.hash(recordId, fields)
}
