// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

/** Tests that all examples provided can be created successfully.
  */
class ExampleTransactionFactoryTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  forEvery(ConfirmationPolicy.values)(policy => {

    s"With policy $policy" when {

      val factory = new ExampleTransactionFactory()(confirmationPolicy = policy)

      forEvery(factory.standardHappyCases)(example => {

        s"creating $example" can {

          "determine the informees" in {
            noException should be thrownBy example.allInformees
          }
          "create the lfTransaction" in {
            noException should be thrownBy example.versionedUnsuffixedTransaction
          }
          "create a well-formed lfTransaction" in {
            noException should be thrownBy example.wellFormedUnsuffixedTransaction
          }
          "create the view decompositions" in {
            noException should be thrownBy example.rootViewDecompositions
          }
          "create the root views" in {
            noException should be thrownBy example.rootViews
          }
          "create views with subviews" in {
            noException should be thrownBy example.viewWithSubviews
          }
          "create divulged contracts" in {
            noException should be thrownBy example.inputContracts
          }
          "create the transaction tree" in {
            noException should be thrownBy example.transactionTree
          }
          "create the transaction id" in {
            example.transactionId should equal(example.transactionTree.transactionId)
          }
          "create the full informee tree" in {
            noException should be thrownBy example.fullInformeeTree
          }
          "create a blinded informee tree" in {
            noException should be thrownBy example.informeeTreeBlindedFor
          }
          "create the transaction view trees with their reinterpreted subaction" in {
            noException should be thrownBy example.reinterpretedSubtransactions
          }
          "create the root transaction view trees" in {
            noException should be thrownBy example.rootTransactionViewTrees
          }
          "create the absolute transaction" in {
            noException should be thrownBy example.versionedSuffixedTransaction
          }
          "create a well-formed absolute transaction" in {
            noException should be thrownBy example.wellFormedSuffixedTransaction
          }
          "with a consistent number of roots" in {
            val roots = example.versionedUnsuffixedTransaction.roots.length

            example.wellFormedUnsuffixedTransaction.unwrap.roots.length shouldBe roots
            example.rootViewDecompositions.length shouldBe roots
            example.rootViews.length shouldBe roots
            example.transactionTree.rootViews.unblindedElements.length shouldBe roots
            example.fullInformeeTree.tree.rootViews.unblindedElements.length shouldBe roots
            example.rootTransactionViewTrees.length shouldBe roots
            example.versionedSuffixedTransaction.roots.length shouldBe roots
            example.wellFormedSuffixedTransaction.unwrap.roots.length shouldBe roots
          }
          "with a consistent number of nodes" in {
            val nodes = example.versionedUnsuffixedTransaction.nodes.size

            example.wellFormedUnsuffixedTransaction.unwrap.nodes.size shouldBe nodes
            example.versionedSuffixedTransaction.nodes.size shouldBe nodes
            example.wellFormedSuffixedTransaction.unwrap.nodes.size shouldBe nodes
          }
          "with a consistent number of views" in {
            val views = example.viewWithSubviews.length

            example.transactionViewTrees.length shouldBe views
          }
          "with consistent transaction root hashes" in {
            example.transactionTree.transactionId shouldEqual example.transactionId
            example.fullInformeeTree.transactionId shouldEqual example.transactionId
            example.informeeTreeBlindedFor._2.transactionId shouldEqual example.transactionId
            forEvery(example.transactionViewTrees) {
              _.transactionId shouldEqual example.transactionId
            }
          }
          "reinterpretations are well-formed" must {
            example.reinterpretedSubtransactions.zipWithIndex.foreach {
              case ((_, (reinterpretedTx, metadata, _keyResolver), _), i) =>
                s"subtransaction $i" in {
                  WellFormedTransaction.normalizeAndAssert(
                    reinterpretedTx,
                    metadata,
                    WithoutSuffixes,
                  )
                }
            }
          }
        }
      })
    }
  })
}
