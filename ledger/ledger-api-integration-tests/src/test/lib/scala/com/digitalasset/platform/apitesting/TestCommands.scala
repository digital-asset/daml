// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.io.File
import java.util

import com.digitalasset.daml.lf.archive.UniversalArchiveReader
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
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.Value.Sum.{Party, Text}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.platform.PlatformApplications
import com.google.protobuf.timestamp.Timestamp
import scalaz.syntax.tag._

class TestTemplateIds(config: PlatformApplications.Config) {
  lazy val defaultDar: File = config.darFiles.head.toFile
  lazy val parsedPackageId: String =
    UniversalArchiveReader().readFile(defaultDar).get.main._1
  lazy val templateIds: TestTemplateIdentifiers = new TestTemplateIdentifiers(parsedPackageId)
}

class TestCommands(config: PlatformApplications.Config) {
  protected val testIds = new TestTemplateIds(config)
  val templateIds = testIds.templateIds

  def buildRequest(
      ledgerId: domain.LedgerId,
      commandId: String,
      commands: Seq[Command],
      party: String,
      let: Timestamp = ledgerEffectiveTime,
      maxRecordTime: Timestamp = maximumRecordTime,
      appId: String = applicationId): SubmitRequest =
    M.submitRequest.update(
      _.commands.commandId := commandId,
      _.commands.ledgerId := ledgerId.unwrap,
      _.commands.applicationId := appId,
      _.commands.party := party,
      _.commands.commands := commands,
      _.commands.ledgerEffectiveTime := let,
      _.commands.maximumRecordTime := maxRecordTime
    )

  def dummyCommands(ledgerId: domain.LedgerId, commandId: String, party: String = "party") =
    buildRequest(
      ledgerId,
      commandId,
      List(
        createWithOperator(templateIds.dummy, party),
        createWithOperator(templateIds.dummyWithParam, party),
        createWithOperator(templateIds.dummyFactory, party)
      ),
      party
    )

  def createWithOperator(templateId: Identifier, party: String = "party") =
    Command(
      Create(CreateCommand(
        Some(templateId),
        Some(Record(Some(templateId), List(RecordField("operator", Some(Value(Party(party))))))))))

  private lazy val oneKilobyteString: String = {
    val numChars = 500 // each char takes 2 bytes for now in Java 8
    val array = new Array[Char](numChars)
    util.Arrays.fill(array, 'a')
    new String(array)
  }

  def oneKbCommand(templateId: Identifier) =
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

  def oneKbCommandRequest(
      ledgerId: domain.LedgerId,
      commandId: String,
      party: String = "party"): SubmitRequest =
    buildRequest(ledgerId, commandId, List(oneKbCommand(templateIds.textContainer)), party)

  def exerciseWithUnit(
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Option[Value] = Some(Value(Sum.Record(Record.defaultInstance)))) =
    Command(Exercise(ExerciseCommand(Some(templateId), contractId, choice, args)))

  def toWait(request: SubmitRequest): SubmitAndWaitRequest = SubmitAndWaitRequest(request.commands)

}
