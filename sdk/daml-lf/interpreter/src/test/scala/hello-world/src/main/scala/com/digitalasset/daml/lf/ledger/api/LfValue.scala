// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.ledger.api

import com.google.protobuf.ByteString
import com.digitalasset.daml.lf.value.{ValueOuterClass => ProtobufConverter}
import scala.collection.immutable.{Map, Set}
import scala.jdk.CollectionConverters._

final case class LfIdentifier(packageId: String = "()", moduleName: String, name: String) {
  def toByteArray: Array[Byte] = {
    s"$packageId:$moduleName:$name".getBytes
  }
}

sealed trait LfValue {
  def toValue[T](implicit converter: LfValueConverterFrom[T]): T = converter.fromLfValue(this)

  def toProtobuf: ProtobufConverter.Value

  def toByteArray: Array[Byte] = {
    toProtobuf.toByteArray
  }
}

// TODO: add missing LfValues
case object LfValueUnit extends LfValue {
  override def toProtobuf: ProtobufConverter.Value = {
    val empty = ProtobufConverter.Value.newBuilder().getUnitBuilder.build()
    ProtobufConverter.Value.newBuilder().setUnit(empty).build()
  }
}
final case class LfValueBool(value: Boolean) extends LfValue {
  override def toProtobuf: ProtobufConverter.Value = {
    ProtobufConverter.Value.newBuilder().setBool(value).build()
  }
}
final case class LfValueInt(int: Int) extends LfValue {
  override def toProtobuf: ProtobufConverter.Value = {
    ProtobufConverter.Value.newBuilder().setInt64(int.toLong).build()
  }
}
final case class LfValueParty(party: String) extends LfValue {
  override def toProtobuf: ProtobufConverter.Value = {
    ProtobufConverter.Value.newBuilder().setParty(party).build()
  }
}
final case class LfValueString(str: String) extends LfValue {
  override def toProtobuf: ProtobufConverter.Value = {
    ProtobufConverter.Value.newBuilder().setText(str).build()
  }
}
final case class LfValueContractId(coid: String) extends LfValue {
  override def toProtobuf: ProtobufConverter.Value = {
    ProtobufConverter.Value.newBuilder().setContractId(ByteString.copyFrom(coid.getBytes)).build()
  }
}
final case class LfValueOptional[V <: LfValue](value: Option[V]) extends LfValue {
  override def toProtobuf: ProtobufConverter.Value = {
    val optional = value.fold(ProtobufConverter.Value.Optional.newBuilder().build())(v =>
      ProtobufConverter.Value.Optional.newBuilder().setValue(v.toProtobuf).build()
    )
    ProtobufConverter.Value.newBuilder().setOptional(optional).build()
  }
}
final case class LfValueSet[V <: LfValue](set: Set[V]) extends LfValue {
  override def toProtobuf: ProtobufConverter.Value = {
    val list =
      ProtobufConverter.Value.List.newBuilder().addAllElements(set.map(_.toProtobuf).asJava).build()
    ProtobufConverter.Value.newBuilder().setList(list).build()
  }
}
final case class LfValueMap[K <: LfValue, V <: LfValue](lfMap: Map[K, V]) extends LfValue {
  override def toProtobuf: ProtobufConverter.Value = {
    val entries = lfMap.map { case (key, value) =>
      ProtobufConverter.Value.Map.Entry
        .newBuilder()
        .setKey(key.toProtobuf)
        .setValue(value.toProtobuf)
        .build()
    }
    val map = ProtobufConverter.Value.Map.newBuilder().addAllEntries(entries.asJava).build()
    ProtobufConverter.Value.newBuilder().setMap(map).build()
  }
}
final case class LfValueRecord(lfRecord: Map[String, LfValue]) extends LfValue {
  override def toProtobuf: ProtobufConverter.Value = {
    val fields = lfRecord.map { case (_, value) =>
      ProtobufConverter.Value.Record.Field.newBuilder().setValue(value.toProtobuf).build()
    }
    val record = ProtobufConverter.Value.Record.newBuilder().addAllFields(fields.asJava).build()
    ProtobufConverter.Value.newBuilder().setRecord(record).build()
  }
}

object LfValueSet {
  def apply[V <: LfValue](elems: V*): LfValueSet[V] = LfValueSet[V](Set(elems: _*))
}

object LfValueRecord {
  def empty: LfValueRecord = LfValueRecord(Map.empty[String, LfValue])

  def apply(fields: (String, LfValue)*): LfValueRecord =
    LfValueRecord(Map(fields: _*))
}
