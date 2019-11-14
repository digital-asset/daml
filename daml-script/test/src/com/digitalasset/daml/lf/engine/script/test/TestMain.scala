// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.test

import akka.actor.ActorSystem
import akka.stream._
import com.typesafe.scalalogging.StrictLogging
import java.io.File
import java.time.Instant
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}
import scalaz.syntax.tag._
import scalaz.syntax.traverse._

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.{FrontStack, FrontStackCons, Numeric}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.services.commands.CommandUpdater

import com.digitalasset.daml.lf.engine.script.Runner

case class Config(ledgerPort: Int, darPath: File, wallclockTime: Boolean)

// We do not use scalatest here since that doesn’t work nicely with
// the client_server_test macro.

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

class TestRunner(val config: Config) extends StrictLogging {
  val applicationId = ApplicationId("DAML Script Test Runner")

  val clientConfig = LedgerClientConfiguration(
    applicationId = applicationId.unwrap,
    ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
    commandClient = CommandClientConfiguration.default,
    sslContext = None
  )
  val ttl = java.time.Duration.ofSeconds(30)
  val commandUpdater = if (config.wallclockTime) {
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
      // The dar package the trigger is in
      dar: Dar[(PackageId, Package)],
      // Identifier of the script value
      scriptId: Identifier,
      assertResult: SValue => Either[String, Unit]) = {

    println(s"---\n$name:")

    val system = ActorSystem("ScriptRunner")
    implicit val sequencer: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("ScriptRunnerPool")(system)
    implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
    implicit val ec: ExecutionContext = system.dispatcher

    val clientF = LedgerClient.singleHost("localhost", config.ledgerPort, clientConfig)

    val runner = new Runner(dar, applicationId, commandUpdater)

    val testFlow: Future[Unit] = for {
      client <- clientF
      result <- runner.run(client, scriptId)
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

case class Test0(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:test0"))
  def runTests() = {
    runner.genericTest(
      "test0",
      dar,
      scriptId,
      result =>
        result match {
          case SRecord(_, _, vals) if vals.size == 5 => {
            for {
              alice <- vals.get(0) match {
                case SParty(alice) => Right(alice)
                case v => Left(s"Expected party but got $v")
              }
              bob <- vals.get(1) match {
                case SParty(bob) => Right(bob)
                case v => Left(s"Expected party but got $v")
              }
              _ <- TestRunner.assertNotEqual(alice, bob, "Allocate party should return a new party")
              ts <- vals.get(2) match {
                case SList(FrontStackCons(t1, FrontStackCons(t2, FrontStack()))) => {
                  Right((t1, t2))
                }
                case v => Left(s"Expected list but got $v")
              }
              (t1, t2) = ts
              _ <- t1 match {
                case SRecord(_, _, vals) if vals.size == 2 =>
                  for {
                    _ <- TestRunner.assertEqual(vals.get(0), SParty(alice), "Alice")
                    _ <- TestRunner.assertEqual(vals.get(1), SParty(bob), "Bob")
                  } yield ()
                case v => Left(s"Expected T but got $v")
              }
              _ <- t2 match {
                case SRecord(_, _, vals) if vals.size == 2 =>
                  for {
                    _ <- TestRunner.assertEqual(vals.get(0), SParty(alice), "Alice")
                    _ <- TestRunner.assertEqual(vals.get(1), SParty(bob), "Bob")
                  } yield ()
                case v => Left(s"Expected T but got $v")
              }
              _ <- TestRunner.assertEqual(vals.get(3), SList(FrontStack.empty), "TProposals")
              c <- vals.get(4) match {
                case SList(FrontStackCons(c, FrontStack())) => {
                  Right(c)
                }
                case v => Left("Expected list but got $v")
              }
              _ <- c match {
                case SRecord(_, _, vals) if vals.size == 2 =>
                  for {
                    _ <- TestRunner.assertEqual(vals.get(0), SParty(alice), "C party")
                    _ <- TestRunner.assertEqual(vals.get(1), SInt64(42), "C value")
                  } yield ()
                case v => Left(s"Expected C but got $v")
              }
            } yield ()
          }
          case v => Left(s"Expected 5-tuple but got $v")
      }
    )
  }
}

case class Test1(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:test1"))
  def runTests() = {
    runner.genericTest(
      "test1",
      dar,
      scriptId,
      result =>
        result match {
          case SNumeric(n) =>
            TestRunner.assertEqual(n, Numeric.assertFromString("2.12000000000"), "Numeric")
          case v => Left(s"Expected SNumeric but got $v")
      }
    )
  }
}

object TestMain {

  private val configParser = new scopt.OptionParser[Config]("daml_script_test") {
    head("daml_script_test")

    opt[Int]("target-port")
      .required()
      .action((p, c) => c.copy(ledgerPort = p))

    arg[File]("<dar>")
      .required()
      .action((d, c) => c.copy(darPath = d))

    opt[Unit]('w', "wall-clock-time")
      .action { (t, c) =>
        c.copy(wallclockTime = true)
      }
      .text("Use wall clock time (UTC). When not provided, static time is used.")
  }

  private val applicationId = ApplicationId("DAML Script Tests")

  def main(args: Array[String]): Unit = {
    configParser.parse(args, Config(0, null, false)) match {
      case None =>
        sys.exit(1)
      case Some(config) =>
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }

        val runner = new TestRunner(config)
        Test0(dar, runner).runTests()
        Test1(dar, runner).runTests()
    }
  }
}
