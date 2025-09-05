// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.examples.java.trailingnone.TrailingNone
import com.digitalasset.canton.util.TestEngine
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ValueParty, ValueRecord}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.util.Optional

class LfEnricherSpec extends AnyWordSpec with BaseTest {

  "LfEnricher" should {

    "remove trailing None fields when enriching values" in {

      val testEngine = new TestEngine(Seq(CantonExamplesPath))
      val underTest = LfEnricher(testEngine.engine, forbidLocalContractIds = false)

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

      // Enrich contract instance
      val enrichedTx = testEngine.consume(underTest.enrichVersionedTransaction(tx))
      checkNoTrailingNoneFields(enrichedTx.nodes.values.collect { case e: Node.Create =>
        e.arg
      }.loneElement)

      // Enrich contract instance
      checkNoTrailingNoneFields(
        testEngine.consume(underTest.enrichContractInstance(inst)).createArg
      )

      // Enrich contract value
      checkNoTrailingNoneFields(
        testEngine.consume(underTest.enrichContractValue(inst.templateId, inst.createArg))
      )

    }
  }

}
