// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File
import java.time.Duration
import scalaz.{-\/, \/-}
import scalaz.syntax.traverse._
import spray.json._

import com.daml.lf.archive.Dar
import com.daml.lf.archive.DarReader
import com.daml.lf.archive.Decode
import com.daml.lf.data.{FrontStack, FrontStackCons, Numeric}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.Ref._
import com.daml.lf.data.Ref.{Party => LedgerParty}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SValue._
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt

import com.daml.lf.engine.script.{Party => ScriptParty, _}

case class Config(
    ledgerPort: Int,
    darPath: File,
    wallclockTime: Boolean,
    auth: Boolean,
    // We use the presence of a root CA as a proxy for whether to enable TLS or not.
    rootCa: Option[File],
)

case class Test0(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:test0"))
  def runTests(): Unit = {
    runner.genericTest(
      "test0",
      scriptId,
      None, {
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
              case v => Left(s"Expected list but got $v")
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
  def runTests(): Unit = {
    runner.genericTest(
      "test1",
      scriptId,
      None, {
        case SNumeric(n) =>
          TestRunner.assertEqual(n, Numeric.assertFromString("2.12000000000"), "Numeric")
        case v => Left(s"Expected SNumeric but got $v")
      }
    )
  }
}

case class Test2(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:test2"))
  def runTests(): Unit = {
    runner.genericTest(
      "test2",
      scriptId,
      Some(JsObject(("p", JsString("Alice")), ("v", JsNumber(42)))), {
        case SInt64(i) => TestRunner.assertEqual(i, 42, "Numeric")
        case v => Left(s"Expected SInt but got $v")
      }
    )
  }
}

case class Test3(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:test3"))
  def runTests(): Unit = {
    runner.genericTest(
      "test3",
      scriptId,
      None, {
        case SUnit => Right(())
        case v => Left(s"Expected SUnit but got $v")
      }
    )
  }
}

case class Test4(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:test4"))
  def runTests(): Unit = {
    runner.genericTest(
      "test4",
      scriptId,
      None, {
        case SRecord(_, _, vals) if vals.size == 2 =>
          TestRunner.assertEqual(vals.get(0), vals.get(1), "ContractIds")
        case v => Left(s"Expected record with 2 fields but got $v")
      }
    )
  }
}

case class TestKey(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:testKey"))
  def runTests(): Unit = {
    runner.genericTest(
      "testKey",
      scriptId,
      None, {
        case SRecord(_, _, vals) if vals.size == 2 =>
          TestRunner.assertEqual(vals.get(0), vals.get(1), "ContractIds")
        case v => Left(s"Expected record with 2 fields but got $v")
      }
    )
  }
}

case class TestCreateAndExercise(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId =
    Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:testCreateAndExercise"))
  def runTests(): Unit = {
    runner.genericTest(
      "testKey",
      scriptId,
      None,
      result => TestRunner.assertEqual(SInt64(42), result, "Exercise result")
    )
  }
}

case class GetTime(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:testGetTime"))
  def runTests() = {
    runner.genericTest(
      "getTime",
      scriptId,
      None,
      result =>
        result match {
          case SRecord(_, _, vals) if vals.size == 2 =>
            for {
              t0 <- TestRunner.assertSTimestamp(vals.get(0))
              t1 <- TestRunner.assertSTimestamp(vals.get(1))
              r <- if (!(t0 <= t1))
                Left(s"Second getTime call $t1 should have happened after first $t0")
              else Right(())
            } yield r
          case v => Left(s"Expected SUnit but got $v")
      }
    )
  }
}

case class SetTime(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:testSetTime"))
  def runTests() = {
    runner.genericTest(
      "tTime",
      scriptId,
      None,
      result =>
        result match {
          case SRecord(_, _, vals) if vals.size == 2 =>
            for {
              t0 <- TestRunner.assertSTimestamp(vals.get(0))
              t1 <- TestRunner.assertSTimestamp(vals.get(1))
              _ <- TestRunner
                .assertEqual(t0, Timestamp.assertFromString("1970-01-01T00:00:00Z"), "t0")
              _ <- TestRunner
                .assertEqual(t1, Timestamp.assertFromString("2000-02-02T00:01:02Z"), "t1")
            } yield ()
          case v => Left(s"Expected Tuple2 but got $v")
      }
    )
  }
}

case class Sleep(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:sleepTest"))
  def runTests() = {
    runner.genericTest(
      "Sleep",
      scriptId,
      None, {
        case SRecord(_, _, vals) if vals.size == 3 =>
          for {
            t0 <- TestRunner.assertSTimestamp(vals.get(0))
            t1 <- TestRunner.assertSTimestamp(vals.get(1))
            t2 <- TestRunner.assertSTimestamp(vals.get(2))
            _ <- if (Duration
                .between(t0.toInstant, t1.toInstant)
                .compareTo(Duration.ofSeconds(1)) < 0 && runner.wallclockTime)
              Left(s"Difference between $t0 and $t1 should be more than 1 second")
            else Right(())
            _ <- if (Duration
                .between(t1.toInstant, t2.toInstant)
                .compareTo(Duration.ofSeconds(2)) < 0 && runner.wallclockTime)
              Left(s"Difference between $t1 and $t2 should be more than 2 seconds")
            else Right(())
          } yield ()
        case v => Left(s"Expected SUnit but got $v")
      }
    )
  }
}

case class PartyIdHintTest(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId =
    Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:partyIdHintTest"))
  def runTests() = {
    runner.genericTest(
      "PartyIdHint",
      scriptId,
      None, {
        case SRecord(_, _, vals) if vals.size == 2 =>
          for {
            _ <- TestRunner.assertEqual(
              vals.get(0),
              SParty(LedgerParty.assertFromString("carol")),
              "Accept party id hint")
            _ <- TestRunner.assertEqual(
              vals.get(1),
              SParty(LedgerParty.assertFromString("dan")),
              "Accept party id hint")
          } yield ()
      }
    )
  }
}

case class ListKnownParties(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId =
    Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:listKnownPartiesTest"))
  def runTests() = {
    runner.genericTest(
      "ListKnownParties",
      scriptId,
      None, {
        case SRecord(_, _, vals) if vals.size == 2 =>
          for {
            newPartyDetails <- vals.get(0) match {
              case SList(FrontStackCons(SRecord(_, _, x), FrontStack())) => Right(x)
              case v => Left(s"Exppected list with one element but got $v")
            }
            _ <- TestRunner.assertEqual(newPartyDetails.get(0), vals.get(1), "new party")
            _ <- TestRunner
              .assertEqual(newPartyDetails.get(1), SOptional(Some(SText("myparty"))), "displayName")
            _ <- TestRunner.assertEqual(newPartyDetails.get(2), SBool(true), "isLocal")
          } yield ()
      }
    )
  }
}

case class TestStack(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId =
    Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:testStack"))
  def runTests() = {
    runner.genericTest(
      "testStack",
      scriptId,
      None,
      // We only want to check that this does not crash so the check here is trivial.
      v => TestRunner.assertEqual(v, SUnit, "Script result")
    )
  }
}

case class TestMaxInboundMessageSize(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId =
    Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:testMaxInboundMessageSize"))
  def runTests(): Unit = {
    runner.genericTest(
      "MaxInboundMessageSize",
      scriptId,
      None, {
        case SUnit => Right(())
        case v => Left(s"Expected SUnit but got $v")
      },
      maxInboundMessageSize = RunnerConfig.DefaultMaxInboundMessageSize * 10,
    )
  }
}

// Runs the example from the docs to make sure it doesn’t produce a runtime error.
case class ScriptExample(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptExample:test"))
  def runTests(): Unit = {
    runner.genericTest(
      "ScriptExample",
      scriptId,
      None, {
        case SUnit => Right(())
        case v => Left(s"Expected SUnit but got $v")
      }
    )
  }
}

case class TraceOrder(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:traceOrder"))
  def traceMsg(msg: String) = s"""[GHC.Base:52]: "$msg""""
  def runTests(): Unit = {
    runner.genericTest(
      "traceOrder",
      scriptId,
      None, {
        case SUnit => Right(())
        case v => Left(s"Expected SUnit but got $v")
      },
      Some(Seq(traceMsg("abc"), traceMsg("def"), traceMsg("abc"), traceMsg("def")))
    )
  }
}

case class TestAuth(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("ScriptTest:auth"))
  def runTests(): Unit = {
    runner.genericTest(
      "auth",
      scriptId,
      Some(JsObject(("_1", JsString("Alice")), ("_2", JsString("Bob")))), {
        case SUnit => Right(())
        case v => Left(s"Expected SInt but got $v")
      }
    )
  }
}

object SingleParticipant {

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
    opt[Unit]("auth")
      .action { (f, c) =>
        c.copy(auth = true)
      }

    opt[File]("cacrt")
      .optional()
      .action((d, c) => c.copy(rootCa = Some(d)))
  }

  private val applicationId = ApplicationId("DAML Script Tests")

  def getToken(parties: List[String], admin: Boolean): String = {
    val payload = AuthServiceJWTPayload(
      ledgerId = None,
      participantId = None,
      exp = None,
      applicationId = None,
      actAs = parties,
      admin = admin,
      readAs = List()
    )
    val header = """{"alg": "HS256", "typ": "JWT"}"""
    val jwt = DecodedJwt[String](header, AuthServiceJWTCodec.writeToString(payload))
    JwtSigner.HMAC256.sign(jwt, "secret") match {
      case -\/(e) => throw new IllegalStateException(e.toString)
      case \/-(a) => a.value
    }
  }

  def main(args: Array[String]): Unit = {
    configParser.parse(args, Config(0, null, false, false, None)) match {
      case None =>
        sys.exit(1)
      case Some(config) =>
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }

        val participantParams = if (config.auth) {
          Participants(
            None,
            List(
              (
                Participant("alice"),
                ApiParameters(
                  "localhost",
                  config.ledgerPort,
                  Some(getToken(List("Alice"), false)))),
              (
                Participant("bob"),
                ApiParameters("localhost", config.ledgerPort, Some(getToken(List("Bob"), false))))
            ).toMap,
            List(
              (ScriptParty("Alice"), Participant("alice")),
              (ScriptParty("Bob"), Participant("bob"))).toMap
          )
        } else {
          Participants(
            Some(ApiParameters("localhost", config.ledgerPort, None)),
            Map.empty,
            Map.empty)
        }

        val runner =
          new TestRunner(participantParams, dar, config.wallclockTime, config.rootCa)
        if (!config.auth) {
          TraceOrder(dar, runner).runTests()
          Test0(dar, runner).runTests()
          Test1(dar, runner).runTests()
          Test2(dar, runner).runTests()
          Test3(dar, runner).runTests()
          Test4(dar, runner).runTests()
          TestKey(dar, runner).runTests()
          TestCreateAndExercise(dar, runner).runTests()
          GetTime(dar, runner).runTests()
          Sleep(dar, runner).runTests()
          PartyIdHintTest(dar, runner).runTests()
          ListKnownParties(dar, runner).runTests()
          TestStack(dar, runner).runTests()
          TestMaxInboundMessageSize(dar, runner).runTests()
          ScriptExample(dar, runner).runTests()
          // Keep this at the end since it changes the time and we cannot go backwards.
          if (!config.wallclockTime) {
            SetTime(dar, runner).runTests()
          }
        } else {
          // We can’t test much with auth since most of our tests rely on party allocation and being
          // able to act as the corresponding party.
          TestAuth(dar, runner).runTests()
        }
    }
  }
}
