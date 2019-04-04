// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.digitalasset.ledger.api.testing.utils.MockMessages.submitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.Create
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.platform.sandbox.{TestDar, TestHelpers}
import com.digitalasset.util.Ctx

trait TransactionServiceHelpers extends TestHelpers {

  val dumyTemplate = Identifier(TestDar.parsedPackageId, moduleName = "Test", entityName = "Dummy")
  val dummyTemplateWithParam =
    Identifier(TestDar.parsedPackageId, moduleName = "Test", entityName = "DummyWithParam")

  def submitRequestWithId(id: String, command: Command, ledgerId: String) =
    submitRequest
      .update(_.commands.commandId := id)
      .update(_.commands.commands := Seq(command))
      .update(_.commands.ledgerId := ledgerId)

  // TODO command tracking should be used here
  def insertCommands(
      submissionFlow: Flow[Ctx[Int, SubmitRequest], Ctx[Int, Completion], _],
      prefix: String,
      i: Int,
      ledgerId: String)(implicit materializer: Materializer) = {
    val arg =
      Record(Some(dumyTemplate), Vector(RecordField("operator", Some(Value(Sum.Party("party"))))))
    val command = Create(CreateCommand(Some(dumyTemplate), Some(arg)))
    Source(for {
      x <- Range(0, i)
      req = submitRequestWithId(s"$prefix-$x", Command(command), ledgerId) //.copy(commands = Some(commands.copy(commands = List(Command(command)))))
      c = Ctx(x, req)
    } yield c).via(submissionFlow).runWith(Sink.ignore)
  }
}
