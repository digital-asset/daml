// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.test

import akka.actor.ActorSystem
import akka.stream._
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.classic.spi.ILoggingEvent
import java.io.File
import java.time.Instant
import scala.collection.mutable.ArrayBuffer
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
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}

import com.digitalasset.daml.lf.engine.script._

object LogCollector {
  val events = new ArrayBuffer[ILoggingEvent]
  def clear(): Unit = events.clear
}

final class LogCollector extends AppenderBase[ILoggingEvent] {

  override def append(e: ILoggingEvent): Unit = {
    LogCollector.events += e
  }
}

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
  def assertSTimestamp(v: SValue) = v match {
    case SValue.STimestamp(t) => Right(t)
    case _ => Left(s"Expected STimestamp but got $v")
  }
}

class TestRunner(
    val participantParams: Participants[ApiParameters],
    val dar: Dar[(PackageId, Package)],
    val wallclockTime: Boolean,
    val token: Option[String],
    val rootCa: Option[File],
) {
  val applicationId = ApplicationId("DAML Script Test Runner")

  val clientConfig = LedgerClientConfiguration(
    applicationId = applicationId.unwrap,
    ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
    commandClient = CommandClientConfiguration.default,
    sslContext = rootCa.flatMap(file =>
      TlsConfiguration.Empty.copy(trustCertCollectionFile = Some(file)).client),
    token = token,
  )
  val ttl = java.time.Duration.ofSeconds(30)
  val timeProvider: TimeProvider =
    if (wallclockTime) TimeProvider.UTC else TimeProvider.Constant(Instant.EPOCH)

  def genericTest[A](
      // test name
      name: String,
      // Identifier of the script value
      scriptId: Identifier,
      inputValue: Option[JsValue],
      assertResult: SValue => Either[String, Unit],
      expectedLog: Option[Seq[String]] = None
  ) = {

    LogCollector.clear()

    println(s"---\n$name:")

    val system = ActorSystem("ScriptRunner")
    implicit val sequencer: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("ScriptRunnerPool")(system)
    implicit val materializer: Materializer = Materializer(system)
    implicit val ec: ExecutionContext = system.dispatcher

    val clientsF = Runner.connect(participantParams, clientConfig)

    val testFlow: Future[Unit] = for {
      clients <- clientsF
      result <- Runner.run(dar, scriptId, inputValue, clients, applicationId, timeProvider)
      _ <- expectedLog match {
        case None => Future.unit
        case Some(expectedLogs) =>
          val logMsgs = LogCollector.events.map(_.getMessage)
          if (expectedLogs.equals(logMsgs)) {
            Future.unit
          } else {
            Future.failed(new RuntimeException(s"Expected logs $expectedLogs but got $logMsgs"))
          }
      }
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
