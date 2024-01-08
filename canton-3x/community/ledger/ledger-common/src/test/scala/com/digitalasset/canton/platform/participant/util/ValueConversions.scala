// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.participant.util

import com.daml.ledger.api.v1.commands.{
  Command,
  CreateCommand,
  ExerciseByKeyCommand,
  ExerciseCommand,
}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.Value.Sum.{
  ContractId,
  Int64,
  List as DamlListValue,
  Numeric,
  Party,
  Text,
  Timestamp,
}
import com.daml.ledger.api.v1.value.{Identifier, List as DamlList, Record, RecordField, Value}

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.language.implicitConversions

object ValueConversions {

  val unit = Value(Sum.Record(Record.defaultInstance))

  implicit class StringValues(val s: String) extends AnyVal {
    def asParty: Value = Value(Party(s))
    def asNumeric: Value = Value(Numeric(s))
    def asText: Value = Value(Text(s))
    def asContractId: Value = Value(ContractId(s))
  }

  implicit class InstantValues(val i: Instant) extends AnyVal {
    def asTime: Value = {
      val micros = TimeUnit.SECONDS.toMicros(i.getEpochSecond) + TimeUnit.NANOSECONDS.toMicros(
        i.getNano.toLong
      )
      Value(Timestamp(micros))
    }
  }

  implicit class BooleanValues(val b: Boolean) extends AnyVal {
    def asBoolean: Value = Value(Value.Sum.Bool(b))
  }

  implicit class LongValues(val i: Long) extends AnyVal {
    def asInt64: Value = Value(Int64(i))
  }

  implicit class LabeledValues(val labeledValues: Seq[(String, Value)]) extends AnyVal {
    def asRecord = Record(None, asRecordFields)

    def asRecordOf(identifier: Identifier) = Record(Some(identifier), asRecordFields)

    def asRecordValue = Value(Value.Sum.Record(asRecord))

    def asRecordValueOf(identifier: Identifier) = Value(Value.Sum.Record(asRecordOf(identifier)))

    def asRecordFields: Seq[RecordField] = {
      labeledValues.map { case (k, v) =>
        RecordField(k, Some(v))
      }
    }
  }

  implicit class ValueSequences(val values: Seq[Value]) extends AnyVal {
    def asList = Value(DamlListValue(DamlList(values)))
  }

  implicit def value2Optional(value: Value): Option[Value] = Some(value)

  implicit class ExerciseCommands(val exercise: ExerciseCommand) extends AnyVal {
    def wrap = Command.of(Command.Command.Exercise(exercise))
  }

  implicit class ExerciseByKeyCommands(val exerciseByKey: ExerciseByKeyCommand) extends AnyVal {
    def wrap = Command.of(Command.Command.ExerciseByKey(exerciseByKey))
  }

  implicit class CreateCommands(val create: CreateCommand) extends AnyVal {
    def wrap = Command.of(Command.Command.Create(create))
  }

}
