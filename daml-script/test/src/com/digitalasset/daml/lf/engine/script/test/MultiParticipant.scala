// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File
import scalaz.syntax.traverse._

import com.daml.lf.archive.Dar
import com.daml.lf.archive.DarReader
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref._
import com.daml.lf.data.Ref.{Party => LedgerParty}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SValue._
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId}

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
      .action { (t, c) =>
        c.copy(wallclockTime = true)
      }
      .text("Use wall clock time (UTC). When not provided, static time is used.")
  }

  private val applicationId = ApplicationId("DAML Script Tests")

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
            (Participant("one"), ApiParameters("localhost", config.ledgerPort)),
            (Participant("two"), ApiParameters("localhost", config.extraParticipantPort))).toMap,
          Map.empty
        )

        val runner = new TestRunner(participantParams, dar, config.wallclockTime, None, None)
        MultiTest(dar, runner).runTests()
        MultiPartyIdHintTest(dar, runner).runTests()
    }
  }
}
