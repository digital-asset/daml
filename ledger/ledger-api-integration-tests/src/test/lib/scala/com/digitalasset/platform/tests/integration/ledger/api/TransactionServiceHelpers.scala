// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.io.File

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.digitalasset.daml.lf.UniversalArchiveReader
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.MockMessages.submitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.Create
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.platform.PlatformApplications
import com.digitalasset.util.Ctx
import org.scalatest.Matchers

import scalaz.syntax.tag._
import scala.concurrent.Future


trait TransactionServiceHelpers extends Matchers {
  lazy val defaultDar: File = PlatformApplications.Config.defaultDarFile

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

  def submitRequestWithId(id: String, command: Command, ledgerId: domain.LedgerId) =
    submitRequest
      .update(_.commands.commandId := id)
      .update(_.commands.commands := Seq(command))
      .update(_.commands.ledgerId := ledgerId.unwrap)

  // TODO command tracking should be used here
  def insertCommands(
      trackingFlow: Flow[Ctx[Int, SubmitRequest], Ctx[Int, Completion], _],
      prefix: String,
      i: Int,
      ledgerId: domain.LedgerId,
      party: String = "party")(implicit materializer: Materializer): Future[Done] = {
    val arg =
      Record(Some(dummyTemplate), Vector(RecordField("operator", Some(Value(Sum.Party(party))))))
    val command = Create(CreateCommand(Some(dummyTemplate), Some(arg)))
    Source(for {
      x <- Range(0, i)
      req = submitRequestWithId(s"$prefix-$x", Command(command), ledgerId)
      c = Ctx(x, req)
    } yield c)
      .via(trackingFlow)
      .runWith(Sink.foreach {
        case Ctx(i, Completion(_, Some(status), transactionId, _)) =>
          status should have('code (0))
          transactionId should not be empty
          ()
      })
  }
}
