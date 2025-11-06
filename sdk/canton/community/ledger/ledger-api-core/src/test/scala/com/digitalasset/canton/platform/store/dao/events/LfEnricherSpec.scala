// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.examples.java.trailingnone.TrailingNone
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.util.TestEngine
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ValueParty, ValueRecord}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.util.Optional
import scala.concurrent.{ExecutionContext, Future}

class LfEnricherSpec extends AnyWordSpec with HasExecutionContext with BaseTest {

  private implicit val ec: ExecutionContext = executorService
  private implicit val lc: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  "LfEnricher" should {

    "remove trailing None fields when enriching values" in {

      val testEngine = new TestEngine(Seq(CantonExamplesPath))
      val underTest = LfEnricher(
        engine = testEngine.engine,
        forbidLocalContractIds = false,
        metrics = LedgerApiServerMetrics.ForTesting,
        packageLoader = new DeduplicatingPackageLoader(),
        loadPackage = (
            packageId,
            _,
        ) => Future.successful(testEngine.packageStore.getArchive(packageId)),
      )

      val alice = "alice"
      val command = new TrailingNone(alice, Optional.empty()).create.commands.loneElement
      val (tx, _) = testEngine.submitAndConsume(command, alice)
      val createNode = tx.nodes.values.collect { case e: Node.Create => e }.loneElement
      val inst = testEngine.suffix(createNode)

      // Contract is not enriched at this point
      inside(inst.createArg) {
        case ValueRecord(None, _) => succeed
        case other => fail(s"Expected ValueRecord, got $other")
      }

      def checkNoTrailingNoneFields(createArg: Value): Assertion =
        inside(createArg) {
          case ValueRecord(Some(_), fields) =>
            inside(fields.toList) {
              case (Some("p"), ValueParty(`alice`)) :: Nil => succeed
              case other => fail(s"Did not expect: $other")
            }
            succeed
          case other => fail(s"Expected enriched contract, got: $other")
        }

      // Enrich contract value
      checkNoTrailingNoneFields(
        underTest.enrichContractValue(inst.templateId, inst.createArg)(using ec, lc).futureValue
      )

    }
  }

}
