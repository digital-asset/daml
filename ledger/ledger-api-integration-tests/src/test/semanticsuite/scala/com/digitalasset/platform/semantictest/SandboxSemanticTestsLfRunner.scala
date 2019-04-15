// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.semantictest

import java.io.{BufferedInputStream, File, FileInputStream}

import com.digitalasset.daml.lf.data.Ref.{PackageId}
import com.digitalasset.daml.lf.engine.testing.SemanticTester
import com.digitalasset.daml.lf.lfpackage.{Ast, Decode}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.services.time.TimeProviderType
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.breakOut

class SandboxSemanticTestsLfRunner
    extends AsyncWordSpec
    with Matchers
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with AkkaBeforeAndAfterAll {

  private val darFile = new File("ledger/ledger-api-integration-tests/SemanticTests.dalf")

  override protected lazy val config: Config = Config.default
    .withDarFile(darFile.toPath)
    .withTimeProvider(TimeProviderType.StaticAllowBackwards)

  // TODO SC delete when implicit disclosure supplied in PostgresLedgerDao
  override protected def fixtureIdsEnabled
    : Set[com.digitalasset.platform.apitesting.LedgerBackend] =
    Set(com.digitalasset.platform.apitesting.LedgerBackend.SandboxInMemory)

  lazy val packages: Map[PackageId, Ast.Package] =
    List(darFile, Config.ghcPrimFileName).map(readPackage)(breakOut)

  s"sandbox launched with $darFile" should {
    for {
      (pkgId, names) <- SemanticTester.scenarios(packages)
      name <- names
    } {
      s"run scenario: $name" in allFixtures { ledger =>
        for {
          _ <- ledger.reset()
          _ <- new SemanticTester(
            parties => new SemanticTestAdapter(ledger, packages, parties.map(_.underlyingString)),
            pkgId,
            packages)
            .testScenario(name)
        } yield succeed
      }
    }
  }
  private def readPackage(f: File): (PackageId, Ast.Package) = {
    val is = new BufferedInputStream(new FileInputStream(f))
    try {
      Decode.decodeArchiveFromInputStream(is)
    } finally {
      is.close()
    }
  }
}
