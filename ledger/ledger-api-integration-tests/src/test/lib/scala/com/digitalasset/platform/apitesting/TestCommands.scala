// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.io.File
import java.util

import com.digitalasset.daml.lf.archive.UniversalArchiveReader
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.MockMessages.{
  applicationId,
  ledgerEffectiveTime,
  maximumRecordTime,
  workflowId
}
import com.digitalasset.ledger.api.testing.utils.{MockMessages => M}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.Create
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand}
import com.digitalasset.ledger.api.v1.value.Value.Sum.{Party, Text}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.testing.TestTemplateIdentifiers
import com.google.protobuf.timestamp.Timestamp
import scalaz.syntax.tag._

class TestTemplateIds(config: PlatformApplications.Config) {
  lazy val defaultDar: File = config.darFiles.head.toFile
  lazy val parsedPackageId: String =
    UniversalArchiveReader().readFile(defaultDar).get.main._1
  lazy val templateIds: TestTemplateIdentifiers = new TestTemplateIdentifiers(parsedPackageId)
}

class TestCommands(config: PlatformApplications.Config) {
  private val testIds = new TestTemplateIds(config)
  private val templateIds = testIds.templateIds

  private def buildRequest(
      ledgerId: domain.LedgerId,
      commandId: String,
      commands: Seq[Command],
      party: String,
      let: Timestamp = ledgerEffectiveTime,
      maxRecordTime: Timestamp = maximumRecordTime,
      appId: String = applicationId,
      workflowId: String = workflowId): SubmitRequest =
    M.submitRequest.update(
      _.commands.commandId := commandId,
      _.commands.ledgerId := ledgerId.unwrap,
      _.commands.applicationId := appId,
      _.commands.workflowId := workflowId,
      _.commands.party := party,
      _.commands.commands := commands,
      _.commands.ledgerEffectiveTime := let,
      _.commands.maximumRecordTime := maxRecordTime
    )

  def dummyCommands(
      ledgerId: domain.LedgerId,
      commandId: String,
      party: String = "party",
      workflowId: String = "") =
    buildRequest(
      ledgerId,
      commandId,
      List(
        createWithOperator(templateIds.dummy, party),
        createWithOperator(templateIds.dummyWithParam, party),
        createWithOperator(templateIds.dummyFactory, party)
      ),
      party,
      workflowId = workflowId
    )

  private def createWithOperator(templateId: Identifier, party: String = "party") =
    Command(
      Create(CreateCommand(
        Some(templateId),
        Some(Record(Some(templateId), List(RecordField("operator", Some(Value(Party(party))))))))))

  private[this] val oneKilobyteString: String = {
    val numChars = 500 // each char takes 2 bytes for now in Java 8
    val array = new Array[Char](numChars)
    util.Arrays.fill(array, 'a')
    new String(array)
  }

  private def oneKbCommand(templateId: Identifier) =
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

}
