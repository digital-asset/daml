// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.ledger.api

import com.digitalasset.daml.lf.value.{ValueOuterClass => ProtobufConverter}
import scala.collection.immutable.{Map, Set}
import scala.jdk.CollectionConverters._

trait LfValueConverterTo[T, V <: LfValue] {
  def toLfValue(t: T): V
}

object LfValueConverterTo {
  implicit val unitConverterTo: LfValueConverterTo[ProtobufConverter.Value, LfValueUnit.type] = {
    value =>
      if (value.hasUnit) {
        LfValueUnit
      } else {
        sys.error(s"invalid unit byte string: $value")
      }
  }

  implicit val boolConverterTo: LfValueConverterTo[ProtobufConverter.Value, LfValueBool] = {
    value =>
      if (value.getSumCase == ProtobufConverter.Value.SumCase.BOOL) {
        LfValueBool(value.getBool)
      } else {
        sys.error(s"invalid boolean byte string: $value")
      }
  }

  implicit val intConverterTo: LfValueConverterTo[ProtobufConverter.Value, LfValueInt] = { value =>
    if (value.getSumCase == ProtobufConverter.Value.SumCase.INT64) {
      LfValueInt(value.getInt64.toInt)
    } else {
      sys.error(s"invalid int byte string: $value")
    }
  }

  implicit val partyConverterTo: LfValueConverterTo[ProtobufConverter.Value, LfValueParty] = {
    value =>
      if (value.getSumCase == ProtobufConverter.Value.SumCase.PARTY) {
        LfValueParty(value.getParty)
      } else {
        sys.error(s"invalid party byte string: $value")
      }
  }

  implicit val stringConverterTo: LfValueConverterTo[ProtobufConverter.Value, LfValueString] = {
    value =>
      if (value.getSumCase == ProtobufConverter.Value.SumCase.TEXT) {
        LfValueString(value.getText)
      } else {
        sys.error(s"invalid text byte string: $value")
      }
  }

  implicit val contractIdConverterTo
      : LfValueConverterTo[ProtobufConverter.Value, LfValueContractId] = { value =>
    if (value.getSumCase == ProtobufConverter.Value.SumCase.CONTRACT_ID) {
      LfValueContractId(value.getContractId.toStringUtf8)
    } else {
      sys.error(s"invalid contract id byte string: $value")
    }
  }

  implicit def optionalConverterTo[V <: LfValue]
      : LfValueConverterTo[ProtobufConverter.Value, LfValueOptional[V]] = { value =>
    if (value.getSumCase == ProtobufConverter.Value.SumCase.OPTIONAL) {
      if (value.getOptional == null) {
        LfValueOptional(None)
      } else {
        LfValueOptional(Some(value.getOptional.getValue.toLfValue[V]))
      }
    } else {
      sys.error(s"invalid optional byte string: $value")
    }
  }

  implicit def setConverterTo[V <: LfValue]
      : LfValueConverterTo[ProtobufConverter.Value, LfValueSet[V]] = { value =>
    if (value.getSumCase == ProtobufConverter.Value.SumCase.LIST) {
      LfValueSet(Set(value.getList.getElementsList.asScala.toSeq.map(_.toLfValue[V]): _*))
    } else {
      sys.error(s"invalid list byte string: $value")
    }
  }

  implicit def mapConverterTo[K <: LfValue, V <: LfValue]
      : LfValueConverterTo[ProtobufConverter.Value, LfValueMap[K, V]] = { value =>
    if (value.getSumCase == ProtobufConverter.Value.SumCase.MAP) {
      LfValueMap(
        Map(
          value.getMap.getEntriesList.asScala.toSeq.map(entry =>
            (entry.getKey().toLfValue[K], entry.getValue().toLfValue[V])
          ): _*
        )
      )
    } else {
      sys.error(s"invalid map byte string: $value")
    }
  }

  implicit val recordConverterTo: LfValueConverterTo[ProtobufConverter.Value, LfValueRecord] = {
    value =>
      if (value.getSumCase == ProtobufConverter.Value.SumCase.RECORD) {
        LfValueRecord(Map(value.getRecord.getFieldsList.asScala.toSeq.zipWithIndex.map {
          case (field, index) => (s"_$index", field.getValue().toLfValue)
        }: _*))
      } else {
        sys.error(s"invalid record byte string: $value")
      }
  }

  implicit class ValueConverterTo(value: ProtobufConverter.Value) {
    def toLfValue[V <: LfValue]: V = {
      val lfValue = value.getSumCase match {
        case ProtobufConverter.Value.SumCase.UNIT =>
          unitConverterTo.toLfValue(value)

        case ProtobufConverter.Value.SumCase.BOOL =>
          boolConverterTo.toLfValue(value)

        case ProtobufConverter.Value.SumCase.INT64 =>
          intConverterTo.toLfValue(value)

        case ProtobufConverter.Value.SumCase.PARTY =>
          partyConverterTo.toLfValue(value)

        case ProtobufConverter.Value.SumCase.TEXT =>
          stringConverterTo.toLfValue(value)

        case ProtobufConverter.Value.SumCase.CONTRACT_ID =>
          contractIdConverterTo.toLfValue(value)

        case ProtobufConverter.Value.SumCase.OPTIONAL =>
          optionalConverterTo.toLfValue(value)

        case ProtobufConverter.Value.SumCase.LIST =>
          setConverterTo.toLfValue(value)

        case ProtobufConverter.Value.SumCase.MAP =>
          mapConverterTo.toLfValue(value)

        case ProtobufConverter.Value.SumCase.RECORD =>
          recordConverterTo.toLfValue(value)

        case _ =>
          sys.error(s"toLfValue unimplemented for: $value")
      }

      lfValue.asInstanceOf[V]
    }
  }

  implicit class ByteArrayConverterTo(bytes: Array[Byte]) {
    def toLfValue[V <: LfValue]: V = {
      val value = ProtobufConverter.Value.parseFrom(bytes)

      value.toLfValue[V]
    }
  }

  implicit class IntConverterTo(int: Int) {
    def toLfValue: LfValueInt = {
      LfValueInt(int)
    }
  }
}
