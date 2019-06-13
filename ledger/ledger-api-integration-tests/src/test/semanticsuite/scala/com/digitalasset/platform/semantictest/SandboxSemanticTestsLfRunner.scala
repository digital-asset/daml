// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.semantictest

import java.io._

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.daml.lf.engine.testing.SemanticTester
import com.digitalasset.daml.lf.types.{Ledger => L}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.services.time.TimeProviderType
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.util.Random

class SandboxSemanticTestsLfRunner
    extends AsyncWordSpec
    with Matchers
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with ScalaFutures
    with AkkaBeforeAndAfterAll {

  private val defaultDarFile = new File(
    rlocation("ledger/ledger-api-integration-tests/SemanticTests.dar"))

  override protected def config: Config =
    Config.default
      .withDarFile(defaultDarFile.toPath)
      .withTimeProvider(TimeProviderType.StaticAllowBackwards)

  lazy val (mainPkgId, packages, darFile) = {
    val df = config.darFiles.head.toFile
    val dar = UniversalArchiveReader().readFile(df).get
    val packages = Map(dar.all.map {
      case (pkgId, archive) => Decode.readArchivePayloadAndVersion(pkgId, archive)._1
    }: _*)
    (dar.main._1, packages, df)
  }

  s"a ledger launched with $darFile" should {
    val runSuffix = if (config.uniqueIdentifiers) "-" + Random.alphanumeric.take(10).mkString else ""
    val partyNameMangler = (partyText: String) => partyText + runSuffix
    val commandIdMangler: ((QualifiedName, Int, L.ScenarioNodeId) => String) =
      (scenario, stepId, nodeId) => s"ledger-api-test-tool-$scenario-$stepId-${nodeId}${runSuffix}"
    for {
      (pkgId, names) <- SemanticTester.scenarios(Map(mainPkgId -> packages(mainPkgId))) // we only care about the main pkg
      name <- names
    } {
      s"run scenario: $name" in allFixtures { ledger =>
        for {
          _ <- new SemanticTester(
            parties =>
              new SemanticTestAdapter(
                ledger,
                packages,
                parties,
                timeoutScaleFactor = this.spanScaleFactor,
                commandSubmissionTtlScaleFactor = config.commandSubmissionTtlScaleFactor),
            pkgId,
            packages,
            partyNameMangler,
            commandIdMangler
          ).testScenario(name)
        } yield succeed
      }
    }
  }
}
