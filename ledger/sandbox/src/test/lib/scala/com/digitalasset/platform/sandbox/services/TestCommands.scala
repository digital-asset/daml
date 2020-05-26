// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import java.io.File
import java.util

import com.daml.lf.archive.DarReader
import com.daml.lf.data.Ref.PackageId
import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.MockMessages.applicationId
import com.daml.ledger.api.testing.utils.{MockMessages => M}
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Command.Command.{Create, Exercise}
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand, ExerciseCommand}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.Value.Sum.{Bool, Party, Text, Timestamp}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value, Variant}
import com.daml.platform.participant.util.ValueConversions._
import com.daml.platform.testing.TestTemplateIdentifiers
import scalaz.syntax.tag._

trait TestCommands {

  protected def darFile: File

  protected def packageId: PackageId = DarReader().readArchiveFromFile(darFile).get.main._1

  protected def templateIds = new TestTemplateIdentifiers(packageId)

  protected def buildRequest(
      ledgerId: domain.LedgerId,
      commandId: String,
      commands: Seq[Command],
      appId: String = applicationId,
  ): SubmitRequest =
    M.submitRequest.update(
      _.commands.commandId := commandId,
      _.commands.ledgerId := ledgerId.unwrap,
      _.commands.applicationId := appId,
      _.commands.commands := commands,
    )

  protected def dummyCommands(
      ledgerId: domain.LedgerId,
      commandId: String,
      party: String = M.party,
  ): SubmitRequest =
    buildRequest(
      ledgerId,
      commandId,
      List(
        createWithOperator(templateIds.dummy, party),
        createWithOperator(templateIds.dummyWithParam, party),
        createWithOperator(templateIds.dummyFactory, party)
      )
    )

  protected def createWithOperator(templateId: Identifier, party: String = M.party): Command =
    Command(
      Create(CreateCommand(
        Some(templateId),
        Some(Record(Some(templateId), List(RecordField("operator", Some(Value(Party(party))))))))))

  private def oneKilobyteString: String = {
    val numChars = 500 // each char takes 2 bytes for now in Java 8
    val array = new Array[Char](numChars)
    util.Arrays.fill(array, 'a')
    new String(array)
  }

  protected def oneKbCommand(templateId: Identifier): Command =
    Command(
      Create(
        CreateCommand(
          Some(templateId),
          Some(
            Record(
              Some(templateId),
              List(
                RecordField("operator", Some(Value(Party("party")))),
                RecordField("text", Some(Value(Text(oneKilobyteString))))
              )))
        )))

  protected def paramShowcaseArgs: Record = {
    val variant = Value(Value.Sum.Variant(Variant(None, "SomeInteger", 1.asInt64)))
    val nestedVariant = Vector("value" -> variant).asRecordValue
    val integerList = Vector(1, 2).map(_.toLong.asInt64).asList
    Record(
      Some(templateIds.parameterShowcase),
      Vector(
        RecordField("operator", "Alice".asParty),
        RecordField("integer", 1.asInt64),
        RecordField("decimal", "1.1".asNumeric),
        RecordField("text", Value(Text("text"))),
        RecordField("bool", Value(Bool(true))),
        RecordField("time", Value(Timestamp(0))),
        RecordField("relTime", 42.asInt64), // RelTime gets now compiled to Integer with the new primitive types
        RecordField("nestedOptionalInteger", nestedVariant),
        RecordField("integerList", integerList),
      )
    )
  }

  protected def paramShowcase: Commands = Commands(
    "ledgerId",
    "workflowId",
    "appId",
    "cmd",
    "Alice",
    Seq(
      Command(Command.Command.Create(
        CreateCommand(Some(templateIds.parameterShowcase), Option(paramShowcaseArgs)))))
  )

  protected def oneKbCommandRequest(ledgerId: domain.LedgerId, commandId: String): SubmitRequest =
    buildRequest(ledgerId, commandId, List(oneKbCommand(templateIds.textContainer)))

  protected def exerciseWithUnit(
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Option[Value] = Some(Value(Sum.Record(Record.defaultInstance)))
  ): Command =
    Command(Exercise(ExerciseCommand(Some(templateId), contractId, choice, args)))

  implicit class SubmitRequestEnhancer(request: SubmitRequest) {
    def toSync: SubmitAndWaitRequest = SubmitAndWaitRequest(request.commands)
  }
}
