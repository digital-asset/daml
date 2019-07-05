// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.semantictest

import java.io._

import akka.stream.scaladsl.Sink
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.daml.lf.engine.testing.SemanticTester
import com.digitalasset.daml.lf.types.{Ledger => L}
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.testing.time_service.GetTimeRequest
import com.digitalasset.platform.apitesting.{MultiLedgerFixture, TestIdsGenerator}
import com.digitalasset.platform.services.time.TimeProviderType
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.exceptions.TestCanceledException
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future
import scala.util.{Failure, Success}

import scalaz.syntax.tag._

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
  protected val testIdsGenerator = new TestIdsGenerator(config)

  lazy val (mainPkgId, packages, darFile) = {
    val df = config.darFiles.head.toFile
    val dar = UniversalArchiveReader().readFile(df).get
    val packages = Map(dar.all.map {
      case (pkgId, archive) => Decode.readArchivePayloadAndVersion(pkgId, archive)._1
    }: _*)
    (dar.main._1, packages, df)
  }

  s"a ledger launched with $darFile" should {
    val scenarioCommandIdMangler: ((QualifiedName, Int, L.ScenarioNodeId) => String) =
      (scenario, stepId, nodeId) =>
        testIdsGenerator.testCommandId(s"ledger-api-test-tool-$scenario-$stepId-${nodeId}")
    for {
      (pkgId, names) <- SemanticTester.scenarios(Map(mainPkgId -> packages(mainPkgId))) // we only care about the main pkg
      name <- names
    } {
      s"run scenario: $name" in allFixtures { ledger =>
        for {
          _ <- ClientAdapter
            .serverStreaming(GetTimeRequest(ledger.ledgerId.unwrap), ledger.timeService.getTime)
            .take(1)
            .map(_.getCurrentTime)
            .runWith(Sink.head)
            .transformWith({
              case Failure(throwable) =>
                Future.failed(
                  new TestCanceledException(
                    "DAML scenario running requires implemented TimeService by the provided Ledger API endpoint.",
                    throwable,
                    3))
              case Success(ts) => Future.successful(ts)
            })
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
            testIdsGenerator.testPartyName,
            testIdsGenerator.untestPartyName,
            scenarioCommandIdMangler
          ).testScenario(name)
        } yield succeed
      }
    }
  }
}
