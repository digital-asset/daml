// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.tests

import java.io.File

import com.daml.ledger.api.rewrite.testtool.infrastructure.{
  LedgerSession,
  LedgerTest,
  LedgerTestSuite
}
import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.digitalasset.daml.lf.engine.testing.SemanticTester
import com.digitalasset.daml.lf.engine.testing.SemanticTester.SemanticTesterError

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

final class SemanticTests(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val loadedPackages = Try {
    val df =
      new File(BazelRunfiles.rlocation("ledger/ledger-api-integration-tests/SemanticTests.dar"))
    val dar = UniversalArchiveReader().readFile(df).get
    dar.main._1 -> Map(dar.all.map {
      case (pkgId, archive) => Decode.readArchivePayloadAndVersion(pkgId, archive)._1
    }: _*)
  }

  override val tests: Vector[LedgerTest] =
    loadedPackages match {
      case Failure(exception) =>
        Vector(LedgerTest("SemanticTests") { implicit context =>
          Future.failed(
            new RuntimeException("Unable to load the semantic tests package", exception))
        })
      case Success((main, packages)) =>
        for (name <- SemanticTester.scenarios(packages)(main).toVector) yield {
          LedgerTest(name.toString) { implicit context =>
            val tester =
              new SemanticTester(
                parties => context.semanticTesterLedger(parties, packages),
                main,
                packages)
            tester.testScenario(name).recover {
              case error @ SemanticTesterError(_, message) =>
                throw new AssertionError(message, error)
            }
          }
        }
    }

}
