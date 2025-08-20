// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services

import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest
import com.daml.ledger.api.v2.commands.Command.Command.{Create, Exercise}
import com.daml.ledger.api.v2.commands.{Command, Commands, CreateCommand, ExerciseCommand}
import com.daml.ledger.api.v2.value.Value.Sum
import com.daml.ledger.api.v2.value.Value.Sum.{Bool, Party, Text, Timestamp}
import com.daml.ledger.api.v2.value.{Identifier, Record, RecordField, Value, Variant}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.ValueConversions.*
import com.digitalasset.canton.ledger.api.MockMessages as M
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref.PackageId

import java.io.File
import java.util

trait TestCommands {

  import TestCommands.SubmitRequestEnhancer

  protected def darFile: File

  protected def packageId: PackageId = DarReader.assertReadArchiveFromFile(darFile).main.pkgId

  protected def templateIds = new TestTemplateIdentifiers(packageId)

  protected def buildRequest(
      commandId: String,
      commands: Seq[Command],
      userId: String = M.userId,
      party: String = M.party,
  ): SubmitRequest =
    M.submitRequest
      .update(
        _.commands.commandId := commandId,
        _.commands.userId := userId,
        _.commands.actAs := Seq(party),
        _.commands.commands := commands,
      )

  protected def dummyCommands(
      commandId: String,
      party: String = M.party,
  ): SubmitRequest =
    buildRequest(
      commandId = commandId,
      commands = List(
        createWithOperator(templateIds.dummy, party),
        createWithOperator(templateIds.dummyWithParam, party),
        createWithOperator(templateIds.dummyFactory, party),
      ),
      party = party,
    )

  protected def dummyMultiPartyCommands(
      commandId: String,
      actAs: Seq[String],
      readAs: Seq[String],
  ): SubmitRequest = {
    // This method returns a multi-party submission, however the Daml contract uses a single party.
    // Pick a random party for the Daml contract (it needs to be one of the submitters).
    val operator = actAs.headOption.getOrElse("")
    dummyCommands(commandId, operator)
      .update(
        _.commands.actAs := actAs,
        _.commands.readAs := readAs,
      )
  }

  protected def createWithOperator(templateId: Identifier, party: String = M.party): Command =
    Command(
      Create(
        CreateCommand(
          Some(templateId),
          Some(Record(Some(templateId), List(RecordField("operator", Some(Value(Party(party))))))),
        )
      )
    )

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
                RecordField("text", Some(Value(Text(oneKilobyteString)))),
              ),
            )
          ),
        )
      )
    )

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
        RecordField(
          "relTime",
          42.asInt64,
        ), // RelTime gets now compiled to Integer with the new primitive types
        RecordField("nestedOptionalInteger", nestedVariant),
        RecordField("integerList", integerList),
      ),
    )
  }

  protected def paramShowcase: Commands = Commands.defaultInstance.copy(
    workflowId = "workflowId",
    userId = "userId",
    commandId = "cmd",
    actAs = Seq("Alice"),
    commands = Seq(
      Command(
        Command.Command.Create(
          CreateCommand(Some(templateIds.parameterShowcase), Option(paramShowcaseArgs))
        )
      )
    ),
  )

  protected def exerciseWithUnit(
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Option[Value] = Some(Value(Sum.Record(Record.defaultInstance))),
  ): Command =
    Command(Exercise(ExerciseCommand(Some(templateId), contractId, choice, args)))

  import language.implicitConversions
  implicit def SubmitRequestEnhancer(request: SubmitRequest): SubmitRequestEnhancer =
    new SubmitRequestEnhancer(request)
}

object TestCommands {
  implicit final class SubmitRequestEnhancer(private val request: SubmitRequest) extends AnyVal {
    def toSync: SubmitAndWaitRequest = SubmitAndWaitRequest(request.commands)
  }
}
