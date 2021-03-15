// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv

import com.daml.lf.transaction._
import org.scalatest.wordspec.AnyWordSpec
import com.daml.lf.value.test.ValueGenerators._
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class NormalizerSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "normalizerTransaction" should {

    "only drops Fetch and Lookup nodes" in {
      forAll(noDanglingRefGenVersionedTransaction) { tx =>
        val createIds = tx.nodes.collect { case (nid, _: Node.NodeCreate[_]) => nid }.toSet
        val exeIds = tx.nodes.collect { case (nid, _: Node.NodeExercises[_, _]) => nid }.toSet
        val preservedIds = createIds union exeIds

        val normalized = Normalizer.normalizeTransaction(CommittedTransaction(tx))

        // version is not modified
        //  normalized.version shouldBe tx.version
        // Fetch and Lookup are filtered out from roots
        normalized.roots shouldBe tx.roots.filter(preservedIds)

        // Fetch and Lookup are filtered out from nodes
        normalized.nodes.keySet shouldBe preservedIds

        // Create nodes are preserved.
        normalized.nodes.filter { case (id, _) => createIds(id) } shouldBe
          tx.nodes.filter { case (id, _) => createIds(id) }

        // Exercises are preserved but Fetch and Lookup nodes are filtered out from their children.
        normalized.nodes.filter { case (id, _) => exeIds(id) } shouldBe
          tx.nodes.collect { case (nid, exe: Node.NodeExercises[_, _]) =>
            nid -> exe.copy(children = exe.children.filter(preservedIds))
          }
      }
    }
  }

}
