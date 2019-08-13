// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.io.File
import java.time.Duration

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.daml.lf.archive.UniversalArchiveReader
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.MockMessages.submitAndWaitRequest
import com.digitalasset.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitRequest
}
import com.digitalasset.ledger.api.v1.commands.Command.Command.Create
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand}
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.ledger.client.services.commands.CommandUpdater
import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.apitesting.{LedgerContext, TestParties}
import org.scalatest.Matchers
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

//TODO: move all the necessary logic from here into TestUtils
class TransactionServiceHelpers(config: PlatformApplications.Config) extends Matchers {
  lazy val defaultDar: File = config.darFiles.head.toFile

  lazy val parsedPackageId: String =
    UniversalArchiveReader().readFile(defaultDar).get.main._1

  val failingCommandId: String = "asyncFail"

  val dummyTemplate = Identifier(parsedPackageId, moduleName = "Test", entityName = "Dummy")
  val dummyTemplateWithParam =
    Identifier(parsedPackageId, moduleName = "Test", entityName = "DummyWithParam")

  val partyValue = Value(Value.Sum.Party("Alice"))
  val wrongFields: Seq[RecordField] = Seq(
    RecordField("operator1", Some(partyValue))
  )
  val wrongArgs = Record(None, wrongFields)
  val wrongCreate: Command = Command()
    .withCreate(
      CreateCommand()
        .withTemplateId(dummyTemplate)
        .withCreateArguments(wrongArgs))

  def submitAndWaitRequestWithId(id: String, command: Command, ledgerId: domain.LedgerId) =
    submitAndWaitRequest
      .update(_.commands.party := TestParties.Alice)
      .update(_.commands.commandId := id)
      .update(_.commands.commands := Seq(command))
      .update(_.commands.ledgerId := ledgerId.unwrap)

  // TODO command tracking should be used here
  def insertCommands(
      submit: SubmitAndWaitRequest => Future[SubmitAndWaitForTransactionIdResponse],
      prefix: String,
      i: Int,
      ledgerId: domain.LedgerId,
      party: String = TestParties.Alice)(implicit materializer: Materializer): Future[Done] = {
    val arg =
      Record(Some(dummyTemplate), Vector(RecordField("operator", Some(Value(Sum.Party(party))))))
    val command = Create(CreateCommand(Some(dummyTemplate), Some(arg)))
    Source(Range(0, i))
      .map(x => submitAndWaitRequestWithId(s"$prefix-$x", Command(command), ledgerId))
      .mapAsync(1)(submit)
      .runWith(Sink.foreach {
        case SubmitAndWaitForTransactionIdResponse(transactionId) =>
          transactionId should not be empty
          ()
      })
  }

  private lazy val defaultTtl = Duration.ofMillis(config.commandConfiguration.commandTtl.toMillis)

  def applyTime(req: SubmitAndWaitRequest, context: LedgerContext, ttl: Duration = defaultTtl)(
      implicit mat: Materializer,
      ec: ExecutionContext): Future[SubmitAndWaitRequest] = {
    context.timeProvider().map { tp =>
      val updater = new CommandUpdater(Some(tp), ttl, true)
      req.copy(commands = req.commands.map(updater.applyOverrides))
    }
  }
}
