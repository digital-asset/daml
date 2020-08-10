// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File
import scala.collection.JavaConverters._
import scalaz.syntax.traverse._

import com.daml.lf.archive.Dar
import com.daml.lf.archive.DarReader
import com.daml.lf.archive.Decode
import com.daml.lf.data.{FrontStack, FrontStackCons}
import com.daml.lf.data.Ref._
import com.daml.lf.data.Ref.{Party => LedgerParty}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.svalue
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import com.daml.daml_lf_dev.DamlLf

import com.daml.lf.engine.script._

case class MultiParticipantConfig(
    ledgerPort: Int,
    extraParticipantPort: Int,
    darPath: File,
    wallclockTime: Boolean)

case class MultiTest(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId = Identifier(dar.main._1, QualifiedName.assertFromString("MultiTest:multiTest"))
  def runTests() = {
    runner.genericTest(
      "multiTest",
      scriptId,
      None,
      result => TestRunner.assertEqual(result, SInt64(42), "Accept return value"))
  }
}

case class MultiPartyIdHintTest(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId =
    Identifier(dar.main._1, QualifiedName.assertFromString("MultiTest:partyIdHintTest"))
  def runTests() = {
    runner.genericTest(
      "partyIdHintTest",
      scriptId,
      None, {
        case SRecord(_, _, vals) if vals.size == 2 =>
          for {
            _ <- TestRunner.assertEqual(
              vals.get(0),
              SParty(LedgerParty.assertFromString("alice")),
              "Accept party id hint on participant one")
            _ <- TestRunner.assertEqual(
              vals.get(1),
              SParty(LedgerParty.assertFromString("bob")),
              "Accept party id hint on participant two")
          } yield ()
      }
    )
  }
}

case class MultiListKnownPartiesTest(dar: Dar[(PackageId, Package)], runner: TestRunner) {
  val scriptId =
    Identifier(dar.main._1, QualifiedName.assertFromString("MultiTest:listKnownPartiesTest"))
  implicit def `SValue Ordering`: Ordering[SValue] = svalue.Ordering
  def runTests() = {
    runner.genericTest(
      "listKnownPartiesTest",
      scriptId,
      None, {
        case SRecord(_, _, vals) if vals.size == 2 =>
          for {
            newPartyDetails1 <- vals.get(0) match {
              case SList(
                  FrontStackCons(
                    SRecord(_, _, x),
                    FrontStackCons(SRecord(_, _, y), FrontStack()))) =>
                // We take the tail since we only want the display name and isLocal
                Right(Seq(x, y).map(fs => Seq(fs.asScala: _*).tail))
              case v => Left(s"Exppected list with two elements but got $v")
            }
            newPartyDetails2 <- vals.get(1) match {
              case SList(
                  FrontStackCons(
                    SRecord(_, _, x),
                    FrontStackCons(SRecord(_, _, y), FrontStack()))) =>
                Right(Seq(x, y).map(fs => Seq(fs.asScala: _*).tail))
              case v => Left(s"Exppected list with two elements but got $v")
            }
            _ <- TestRunner
              .assertEqual(
                newPartyDetails1,
                Seq(
                  Seq[SValue](SOptional(Some(SText("p1"))), SBool(true)),
                  Seq[SValue](SOptional(Some(SText("p2"))), SBool(false))),
                "partyDetails1")
            _ <- TestRunner
              .assertEqual(
                newPartyDetails2,
                Seq(
                  Seq[SValue](SOptional(Some(SText("p1"))), SBool(false)),
                  Seq[SValue](SOptional(Some(SText("p2"))), SBool(true))),
                "partyDetails2")
          } yield ()
      }
    )
  }
}

object MultiParticipant {

  private val configParser = new scopt.OptionParser[MultiParticipantConfig]("daml_script_test") {
    head("daml_script_test")

    opt[Int]("target-port")
      .required()
      .action((p, c) => c.copy(ledgerPort = p))

    opt[Int]("extra-participant-port")
      .required()
      .action((p, c) => c.copy(extraParticipantPort = p))

    arg[File]("<dar>")
      .required()
      .action((d, c) => c.copy(darPath = d))

    opt[Unit]('w', "wall-clock-time")
      .action { (_, c) =>
        c.copy(wallclockTime = true)
      }
      .text("Use wall clock time (UTC). When not provided, static time is used.")
  }

  def main(args: Array[String]): Unit = {
    configParser.parse(args, MultiParticipantConfig(0, 0, null, false)) match {
      case None =>
        sys.exit(1)
      case Some(config) =>
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }

        val participantParams = Participants(
          None,
          Seq(
            (Participant("one"), ApiParameters("localhost", config.ledgerPort, None, None)),
            (
              Participant("two"),
              ApiParameters("localhost", config.extraParticipantPort, None, None))
          ).toMap,
          Map.empty
        )

        val runner = new TestRunner(participantParams, dar, config.wallclockTime, None)
        MultiTest(dar, runner).runTests()
        MultiPartyIdHintTest(dar, runner).runTests()
        MultiListKnownPartiesTest(dar, runner).runTests()
    }
  }
}
