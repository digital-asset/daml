// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v2.value.*
import com.daml.ledger.api.v2.value.Value.{Sum, toJavaProto}
import com.daml.ledger.javaapi.data.DamlRecord

// TODO(#27163): This can be removed once DamlRecord.toValue does not produce a Record with trailing None values
object RemoveTrailingNone {

  private def removeTrailingNone(value: Value): Value = {
    val sum = value.sum match {
      case Sum.Record(record) =>
        val n = record.fields.reverseIterator
          .dropWhile(_.value match {
            case None => true
            case Some(Value(Value.Sum.Optional(Optional(None)))) => true
            case _ => false
          })
          .size
        Sum.Record(
          record.copy(fields =
            record.fields.take(n).map(f => f.copy(value = f.value.map(removeTrailingNone)))
          )
        )
      case Sum.Variant(variant) =>
        Sum.Variant(variant.clearVariantId.copy(value = variant.value.map(removeTrailingNone)))
      case Sum.List(list) => Sum.List(List(list.elements.map(removeTrailingNone)))
      case Sum.Optional(optional) =>
        Sum.Optional(Optional(optional.value.map(removeTrailingNone)))
      case Sum.GenMap(map) =>
        Sum.GenMap(GenMap(map.entries.map { case GenMap.Entry(k, v) =>
          GenMap.Entry(k.map(removeTrailingNone), v.map(removeTrailingNone))
        }))
      case Sum.TextMap(map) =>
        Sum.TextMap(TextMap(map.entries.map { case TextMap.Entry(k, v) =>
          TextMap.Entry(k, v.map(removeTrailingNone))
        }))
      case _ => value.sum
    }
    Value(sum)
  }

  implicit class Implicits(record: DamlRecord) {
    def withoutTrailingNoneFields: DamlRecord = {
      val withTrailing = Value.fromJavaProto(record.toProto)
      val withoutTrailing = removeTrailingNone(withTrailing)
      DamlRecord.fromProto(toJavaProto(withoutTrailing).getRecord)
    }
  }
}
