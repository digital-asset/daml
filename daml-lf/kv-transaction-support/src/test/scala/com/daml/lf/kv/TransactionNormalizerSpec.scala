// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv

import com.daml.lf.transaction._
import com.daml.lf.transaction.Node._
import org.scalatest.wordspec.AnyWordSpec
import com.daml.lf.value.test.ValueGenerators._
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.daml.lf.data.ImmArray

class TransactionNormalizerSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "normalizerTransaction" should {

    "only keeps Create and Exercise nodes" in {
      forAll(noDanglingRefGenVersionedTransaction) { tx =>
        val normalized = TransactionNormalizer.normalize(CommittedTransaction(tx))

        val nidsBefore: Set[NodeId] = tx.nodes.keySet
        val nidsAfter: Set[NodeId] = normalized.nodes.keySet

        // Every kept nid existed before
        assert(nidsAfter.forall(nid => nidsBefore.contains(nid)))

        // The normalized nodes mapping does not contain anything unreachanble
        nidsAfter shouldBe normalized.reachableNodeIds

        // Only create/exercise nodes are kept
        assert(
          nidsAfter
            .map(normalized.nodes(_))
            .forall(isCreateOrExercise)
        )

        // Everything kept is unchanged (except children may be dropped)
        def unchangedByNormalization(nid: NodeId): Boolean = {
          val before = tx.nodes(nid)
          val after = normalized.nodes(nid)
          // the node is unchanged when disregarding the children
          nodeSansChildren(after) == nodeSansChildren(before)
          // children can be lost, but nothing else
          val beforeChildren = nodeChildren(before)
          val afterChildren = nodeChildren(after)
          afterChildren.forall(beforeChildren.contains(_))
        }
        assert(
          nidsAfter.forall(unchangedByNormalization)
        )

        // Does a Nid reference a create/exercise node (in the before tx) ?
        def isNidCE(nid: NodeId): Boolean = {
          isCreateOrExercise(tx.nodes(nid))
        }

        // Is a Nid kept in the after transaction ?
        def isKept(nid: NodeId): Boolean = {
          nidsAfter.contains(nid)
        }

        // Create/exercise root nodes are kept
        assert(
          tx.roots.toList
            .filter(isNidCE)
            .forall(isKept)
        )

        // Create/exercise children of kept nodes should also be kept
        assert(
          nidsAfter
            .flatMap(nid => nodeChildren(tx.nodes(nid)))
            .filter(isNidCE)
            .forall(isKept)
        )

      }
    }
  }

  def isCreateOrExercise(node: GenNode): Boolean = {
    node match {
      case _: NodeExercises => true
      case _: NodeCreate => true
      case _ => false
    }
  }

  def nodeSansChildren(node: GenNode): GenNode = {
    node match {
      case exe: NodeExercises => exe.copy(children = ImmArray.Empty)
      case _ => node
    }
  }

  def nodeChildren(node: GenNode): List[NodeId] = {
    node match {
      case exe: NodeExercises => exe.children.toList
      case _ => List()
    }
  }

}
