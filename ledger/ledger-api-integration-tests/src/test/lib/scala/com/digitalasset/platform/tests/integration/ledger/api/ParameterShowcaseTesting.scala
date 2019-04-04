// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import com.digitalasset.ledger.api.v1.value.Value.Sum.{Bool, Text, Timestamp}
import com.digitalasset.ledger.api.v1.value.{
  Identifier,
  Record,
  RecordField,
  Value,
  Variant,
  Optional
}
import com.digitalasset.platform.participant.util.ValueConversions._

trait ParameterShowcaseTesting {

  protected val integerListRecordLabel = "integerList"

  protected def paramShowcaseArgs(packageId: String): Vector[RecordField] = {
    val variant = Value(
      Value.Sum.Variant(
        Variant(
          Some(Identifier(packageId, "Test.OptionalInteger", "Test", "OptionalInteger")),
          "SomeInteger",
          1.asInt64)))
    val nestedVariant = Vector("value" -> variant)
      .asRecordValueOf(
        Identifier(packageId, "Test.NestedOptionalInteger", "Test", "NestedOptionalInteger"))
    val integerList = Vector(1, 2).map(_.toLong.asInt64).asList
    val optionalText = Optional(Value(Text("foo")))
    Vector(
      RecordField("operator", "party".asParty),
      RecordField("integer", 1.asInt64),
      RecordField("decimal", "1.1".asDecimal),
      RecordField("text", Value(Text("text"))),
      RecordField("bool", Value(Bool(true))),
      RecordField("time", Value(Timestamp(0))),
      RecordField("nestedOptionalInteger", nestedVariant),
      RecordField(integerListRecordLabel, integerList),
      RecordField("optionalText", Some(Value(Value.Sum.Optional(optionalText))))
    )
  }

  val paramShowcaseArgsWithoutLabels = {
    val variant = Value(Value.Sum.Variant(Variant(None, "SomeInteger", 1.asInt64)))
    val nestedVariant = Vector("" -> variant).asRecordValue
    val integerList = Vector(1, 2).map(_.toLong.asInt64).asList
    val optionalText = Optional(Value(Text("foo")))
    Vector(
      RecordField("", "party".asParty),
      RecordField("", 1.asInt64),
      RecordField("", "1.1".asDecimal),
      RecordField("", Value(Text("text"))),
      RecordField("", Value(Bool(true))),
      RecordField("", Value(Timestamp(0))),
      RecordField("", nestedVariant),
      RecordField("", integerList),
      RecordField("", Some(Value(Value.Sum.Optional(optionalText))))
    )
  }

  protected def paramShowcaseArgumentsToChoice1Argument(args: Record): Value =
    Value(Value.Sum.Record(args.update(_.fields.modify { originalFields =>
      originalFields.collect {
        // prune "operator" -- we do not have it in the choice params
        case original if original.label != "operator" =>
          val newLabel = "new" + original.label.capitalize
          RecordField(newLabel, original.value)
      }
    })))
}
