// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.test

import akka.actor.ActorSystem
import akka.stream._
import com.typesafe.scalalogging.StrictLogging
import java.time.Instant
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}
import scalaz.syntax.tag._
import spray.json._

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.ledger.client.services.commands.CommandUpdater

import com.digitalasset.daml.lf.engine.script._

object TestRunner {
  def assertEqual[A](actual: A, expected: A, note: String) = {
    if (actual == expected) {
      Right(())
    } else {
      Left(s"$note: Expected $expected but got $actual")
    }
  }
  def assertNotEqual[A](actual: A, expected: A, note: String) = {
    if (actual != expected) {
      Right(())
    } else {
      Left(s"$note: Expected $expected and $actual to be different")
    }
  }
}

class TestRunner(
    val participantParams: Participants[ApiParameters],
    val dar: Dar[(PackageId, Package)],
    val wallclockTime: Boolean)
    extends StrictLogging {
  val applicationId = ApplicationId("DAML Script Test Runner")

  val clientConfig = LedgerClientConfiguration(
    applicationId = applicationId.unwrap,
    ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
    commandClient = CommandClientConfiguration.default,
    sslContext = None
  )
  val ttl = java.time.Duration.ofSeconds(30)
  val commandUpdater = if (wallclockTime) {
    new CommandUpdater(timeProviderO = Some(TimeProvider.UTC), ttl = ttl, overrideTtl = true)
  } else {
    new CommandUpdater(
      timeProviderO = Some(TimeProvider.Constant(Instant.EPOCH)),
      ttl = ttl,
      overrideTtl = true)
  }

  def genericTest[A](
      // test name
      name: String,
      // Identifier of the script value
      scriptId: Identifier,
      inputValue: Option[JsValue],
      assertResult: SValue => Either[String, Unit]) = {

    println(s"---\n$name:")

    val system = ActorSystem("ScriptRunner")
    implicit val sequencer: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("ScriptRunnerPool")(system)
    implicit val materializer: Materializer = Materializer(system)
    implicit val ec: ExecutionContext = system.dispatcher

    val clientsF = Runner.connect(participantParams, clientConfig)

    val runner = new Runner(dar, applicationId, commandUpdater)

    val testFlow: Future[Unit] = for {
      clients <- clientsF
      result <- runner.run(clients, scriptId, inputValue)
      _ <- assertResult(result) match {
        case Left(err) =>
          Future.failed(new RuntimeException(s"Assertion on script result failed: $err"))
        case Right(()) => Future.unit
      }
    } yield ()
    testFlow.onComplete({
      case Success(_) => {
        system.terminate
        println(s"Test $name succeeded")
      }
      case Failure(err) => {
        println(s"Test $name failed: $err")
        sys.exit(1)
      }
    })
    Await.result(testFlow, Duration.Inf)
  }
}
