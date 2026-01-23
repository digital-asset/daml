// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.data.FullTransactionViewTree
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class TransactionProcessingStepsTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  "TransactionProcessingSteps.CommonData" when {

    val factory = new ExampleTransactionFactory()()
    val diverseTrees: Seq[FullTransactionViewTree] =
      factory.standardHappyCases.take(3).flatMap(_.rootTransactionViewTrees)
    val tvt = diverseTrees.head
    val expected = TransactionProcessingSteps.tryCommonData(NonEmpty.mk(Seq, tvt))

    "Allow the construction from consistent views" in {
      TransactionProcessingSteps.tryCommonData(NonEmpty(Seq, tvt, tvt)) shouldBe expected
    }

    "Throw an exception if the common data is inconsistent between views" in {
      val ex = intercept[IllegalArgumentException](
        TransactionProcessingSteps.tryCommonData(NonEmptyUtil.fromUnsafe(diverseTrees))
      )
      ex.getMessage should include("Found several different transaction IDs")
    }

  }

}
