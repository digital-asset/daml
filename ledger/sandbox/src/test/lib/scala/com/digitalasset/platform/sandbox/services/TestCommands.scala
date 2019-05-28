// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.io.File
import java.time.Instant
import java.util

import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.MockMessages.{
  applicationId,
  ledgerEffectiveTime,
  maximumRecordTime
}
import com.digitalasset.ledger.api.testing.utils.{MockMessages => M}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.{Create, Exercise}
import com.digitalasset.ledger.api.v1.commands.{Command, Commands, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.Value.Sum.{Bool, Party, Text, Timestamp}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value, Variant}
import com.digitalasset.platform.sandbox.TestTemplateIdentifiers
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.google.protobuf.timestamp.{Timestamp => GTimestamp}

import scalaz.syntax.tag._

// TODO(mthvedt): Delete this old copy when we finish migrating to ledger-api-integration-tests.
@SuppressWarnings(Array("org.wartremover.warts.Any"))
trait TestCommands {

  protected def darFile: File

  @transient
  protected def templateIds = {
    val container = DamlPackageContainer(List(darFile))
    val testArchive = container.archives.head._2
    new TestTemplateIdentifiers(testArchive.getHash)
  }

  protected def buildRequest(
      ledgerId: domain.LedgerId,
      commandId: String,
      commands: Seq[Command],
      let: GTimestamp = ledgerEffectiveTime,
      maxRecordTime: GTimestamp = maximumRecordTime,
      appId: String = applicationId) =
    M.submitRequest.update(
      _.commands.commandId := commandId,
      _.commands.ledgerId := ledgerId.unwrap,
      _.commands.applicationId := appId,
      _.commands.commands := commands,
      _.commands.ledgerEffectiveTime := let,
      _.commands.maximumRecordTime := maxRecordTime
    )

  protected def dummyCommands(ledgerId: domain.LedgerId, commandId: String) =
    buildRequest(
      ledgerId,
      commandId,
      List(
        createWithOperator(templateIds.dummy),
        createWithOperator(templateIds.dummyWithParam),
        createWithOperator(templateIds.dummyFactory)
      )
    )

  protected def createWithOperator(templateId: Identifier) =
    Command(
      Create(
        CreateCommand(
          Some(templateId),
          Some(
            Record(Some(templateId), List(RecordField("operator", Some(Value(Party("party"))))))))))

  private def oneKilobyteString: String = {
    val numChars = 500 // each char takes 2 bytes for now in Java 8
    val array = new Array[Char](numChars)
    util.Arrays.fill(array, 'a')
    new String(array)
  }

  protected def oneKbCommand(templateId: Identifier) =
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

  import com.digitalasset.platform.participant.util.ValueConversions._
  private def integerListRecordLabel = "integerList"
  protected def paramShowcaseArgs: Record = {
    val variant = Value(Value.Sum.Variant(Variant(None, "SomeInteger", 1.asInt64)))
    val nestedVariant = Vector("value" -> variant).asRecordValue
    val integerList = Vector(1, 2).map(_.toLong.asInt64).asList
    Record(
      Some(templateIds.parameterShowcase),
      Vector(
        RecordField("operator", "Alice".asParty),
        RecordField("integer", 1.asInt64),
        RecordField("decimal", "1.1".asDecimal),
        RecordField("text", Value(Text("text"))),
        RecordField("bool", Value(Bool(true))),
        RecordField("time", Value(Timestamp(0))),
        RecordField("relTime", 42.asInt64), // RelTime gets now compiled to Integer with the new primitive types
        RecordField("nestedOptionalInteger", nestedVariant),
        RecordField(integerListRecordLabel, integerList)
      )
    )
  }
  protected def paramShowcase = Commands(
    "ledgerId",
    "workflowId",
    "appId",
    "cmd",
    "Alice",
    Some(TimestampConversion.fromInstant(Instant.now)),
    Some(TimestampConversion.fromInstant(Instant.now)),
    Seq(
      Command(Command.Command.Create(
        CreateCommand(Some(templateIds.parameterShowcase), Option(paramShowcaseArgs)))))
  )

  protected def oneKbCommandRequest(ledgerId: domain.LedgerId, commandId: String) =
    buildRequest(ledgerId, commandId, List(oneKbCommand(templateIds.textContainer)))

  protected def exerciseWithUnit(
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Option[Value] = Some(Value(Sum.Record(Record.defaultInstance)))) =
    Command(Exercise(ExerciseCommand(Some(templateId), contractId, choice, args)))

  implicit class SubmitRequestEnhancer(request: SubmitRequest) {
    def toSync: SubmitAndWaitRequest = SubmitAndWaitRequest(request.commands)
  }

}
