// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.refinements.ApiTypes.ContractId
import com.daml.lf.data.Time.Timestamp
import com.daml.script.export.TreeUtils.{Action, SetTime, SubmitSimpleSingle}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class ActionsFromTreesSpec extends AnyFreeSpec with Matchers {
  "fromTrees" - {
    "setTime enabled" - {
      "empty sequence" in {
        Action.fromTrees(Seq.empty, setTime = true) shouldBe empty
      }
      "single transaction" in {
        val t1 = Timestamp.assertFromInstant(Instant.parse("1990-01-01T01:00:00Z"))
        val trees = Seq(
          TestData
            .Tree(
              Seq(
                TestData.Created(ContractId("cid1"))
              ),
              timestamp = t1,
            )
            .toTransactionTree
        )
        val actions = Action.fromTrees(trees, setTime = true)
        actions should have length 2
        actions(0) shouldBe SetTime(t1)
        actions(1) shouldBe a[SubmitSimpleSingle]
      }
      "multipe transaction at same time" in {
        val t1 = Timestamp.assertFromInstant(Instant.parse("1990-01-01T01:00:00Z"))
        val trees = Seq(
          TestData
            .Tree(
              Seq(
                TestData.Created(ContractId("cid1"))
              ),
              timestamp = t1,
            )
            .toTransactionTree,
          TestData
            .Tree(
              Seq(
                TestData.Created(ContractId("cid2"))
              ),
              timestamp = t1,
            )
            .toTransactionTree,
        )
        val actions = Action.fromTrees(trees, setTime = true)
        actions should have length 3
        actions(0) shouldBe SetTime(t1)
        actions(1) shouldBe a[SubmitSimpleSingle]
        actions(2) shouldBe a[SubmitSimpleSingle]
      }
      "multipe transaction at different times" in {
        val t1 = Timestamp.assertFromInstant(Instant.parse("1990-01-01T01:00:00Z"))
        val t2 = Timestamp.assertFromInstant(Instant.parse("1990-01-02T01:00:00Z"))
        val trees = Seq(
          TestData
            .Tree(
              Seq(
                TestData.Created(ContractId("cid1"))
              ),
              timestamp = t1,
            )
            .toTransactionTree,
          TestData
            .Tree(
              Seq(
                TestData.Created(ContractId("cid2"))
              ),
              timestamp = t2,
            )
            .toTransactionTree,
        )
        val actions = Action.fromTrees(trees, setTime = true)
        actions should have length 4
        actions(0) shouldBe SetTime(t1)
        actions(1) shouldBe a[SubmitSimpleSingle]
        actions(2) shouldBe SetTime(t2)
        actions(3) shouldBe a[SubmitSimpleSingle]
      }
    }
    "setTime disabled" - {
      "multipe transaction at different times" in {
        val t1 = Timestamp.assertFromInstant(Instant.parse("1990-01-01T01:00:00Z"))
        val t2 = Timestamp.assertFromInstant(Instant.parse("1990-01-02T01:00:00Z"))
        val trees = Seq(
          TestData
            .Tree(
              Seq(
                TestData.Created(ContractId("cid1"))
              ),
              timestamp = t1,
            )
            .toTransactionTree,
          TestData
            .Tree(
              Seq(
                TestData.Created(ContractId("cid2"))
              ),
              timestamp = t2,
            )
            .toTransactionTree,
        )
        val actions = Action.fromTrees(trees, setTime = false)
        actions should have length 2
        actions(0) shouldBe a[SubmitSimpleSingle]
        actions(1) shouldBe a[SubmitSimpleSingle]
      }
    }
  }
}
