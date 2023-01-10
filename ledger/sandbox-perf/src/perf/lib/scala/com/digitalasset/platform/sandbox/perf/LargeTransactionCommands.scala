// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import com.daml.ledger.api.v1.commands.Command.Command
import com.daml.ledger.api.v1.commands.{CreateCommand, ExerciseCommand}
import com.daml.ledger.api.v1.value.Value.{Sum => P}
import com.daml.ledger.api.v1.value.{Identifier, List, Record, RecordField, Value}

import scala.annotation.nowarn

object LargeTransactionCommands {

  private[this] val party = P.Party("party")

  def createCommand(templateId: Identifier): Command.Create = {
    val fields = Seq(RecordField("party", Some(Value(party))))
    Command.Create(
      CreateCommand(
        templateId = Some(templateId),
        createArguments = Some(Record(Some(templateId), fields)),
      )
    )
  }

  def rangeOfIntsCreateCommand(
      templateId: Identifier,
      start: Int,
      step: Int,
      number: Int,
  ): Command.Create = {
    val fields = Seq(
      RecordField("party", Some(Value(party))),
      RecordField("start", Some(Value(P.Int64(start.toLong)))),
      RecordField("step", Some(Value(P.Int64(step.toLong)))),
      RecordField("size", Some(Value(P.Int64(number.toLong)))),
    )
    Command.Create(
      CreateCommand(
        templateId = Some(templateId),
        createArguments = Some(Record(Some(templateId), fields)),
      )
    )
  }

  def exerciseCommand(
      templateId: Identifier,
      contractId: String,
      choice: String,
      arguments: Option[Value],
  ): Command.Exercise = {
    val choiceArgument: Some[Value] = arguments match {
      case None => Some(emptyChoiceArgs(choice))
      case x @ Some(_) => x
    }
    Command.Exercise(
      ExerciseCommand(
        templateId = Some(templateId),
        contractId = contractId,
        choice = choice,
        choiceArgument = choiceArgument,
      )
    )
  }

  def exerciseSizeCommand(
      templateId: Identifier,
      contractId: String,
      size: Int,
  ): Command.Exercise = {
    val choice = "Size"
    val choiceId = Identifier(
      packageId = templateId.packageId,
      moduleName = templateId.moduleName,
      entityName = choice,
    )

    val scalaList = scala.List.range(0L, size.toLong).map(x => Value(P.Int64(x)))
    val damlList = P.List(List(scalaList))
    val listArg = RecordField("list", Some(Value(damlList)))
    val args = Value(P.Record(Record(recordId = Some(choiceId), fields = scala.List(listArg))))

    exerciseCommand(templateId, contractId, choice, Some(args))
  }

  /** - daml 1.1 encodes empty choices as a unit `Value(Sum.Unit(Empty())))`
    * - daml 1.2 prior to DEL-6677 fix, encodes empty choices as variants: `Value(P.Variant(Variant(None, choice, Option(Value(P.Unit(Empty()))))))`
    * - daml 1.2 with DEL-6677 fix, encodes empty choices as empty records: `Value(P.Record(Record(recordId = None, fields = Seq())))`
    *
    * this implementation is for daml 1.2 prior to DEL-6677 fix.
    * once daml-tools is upgrade, it has to be the 3rd option from above.
    */
  @nowarn("msg=parameter value choice .* is never used") // part of public API
  def emptyChoiceArgs(choice: String): Value = {
    Value(P.Record(Record(recordId = None, fields = Seq())))
  }
}
