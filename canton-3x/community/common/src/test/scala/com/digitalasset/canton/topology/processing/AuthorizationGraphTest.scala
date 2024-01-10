// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.AuthorizedNamespaceDelegation
import com.digitalasset.canton.topology.transaction.NamespaceDelegation
import com.digitalasset.canton.topology.{Namespace, TestingOwnerWithKeys}
import org.scalatest.wordspec.AnyWordSpec

class AuthorizationGraphTest extends AnyWordSpec with BaseTestWordSpec {

  import com.digitalasset.canton.topology.DefaultTestIdentities.*

  private class Fixture {

    val factory = new TestingOwnerWithKeys(domainManager, loggerFactory, directExecutionContext)
    import factory.SigningKeys.*
    val namespace = Namespace(key1.fingerprint)

    def mkGraph = new AuthorizationGraph(namespace, extraDebugInfo = true, loggerFactory)
    def mkAuth(nsd: NamespaceDelegation, key: SigningPublicKey): AuthorizedNamespaceDelegation = {
      val tx = factory.mkAdd(nsd, key)
      AuthorizedTopologyTransaction(tx.uniquePath, nsd, tx)
    }
    val nsk1k1 = mkAuth(NamespaceDelegation(namespace, key1, isRootDelegation = true), key1)
    val nsk2k1 = mkAuth(NamespaceDelegation(namespace, key2, isRootDelegation = true), key1)
    val nsk2k1p = mkAuth(NamespaceDelegation(namespace, key2, isRootDelegation = true), key1)
    val nsk3k2 = mkAuth(NamespaceDelegation(namespace, key3, isRootDelegation = true), key2)
    val nsk1k2 =
      mkAuth(NamespaceDelegation(namespace, key1, isRootDelegation = true), key2) // cycle
    val nsk3k1 = mkAuth(NamespaceDelegation(namespace, key3, isRootDelegation = false), key1)
  }
  private lazy val fixture = new Fixture()
  private def check(
      graph: AuthorizationGraph,
      key: SigningPublicKey,
      requireRoot: Boolean,
      should: Boolean,
  ) = {
    graph.isValidAuthorizationKey(key.fingerprint, requireRoot = requireRoot) shouldBe should
  }

  "authorization graph" should {
    import fixture.*
    import fixture.factory.SigningKeys.*
    "add simple" in {
      val graph = mkGraph
      graph.add(nsk1k1)
      check(graph, key1, requireRoot = true, should = true)
      check(graph, key1, requireRoot = false, should = true)
      check(graph, key2, requireRoot = false, should = false)
    }
    "support longer chains" in {
      val graph = mkGraph
      graph.add(nsk1k1)
      graph.add(nsk2k1)
      graph.add(nsk3k2)
      check(graph, key2, requireRoot = false, should = true)
      check(graph, key3, requireRoot = false, should = true)
    }

    "support removal" in {
      val graph = mkGraph
      graph.add(nsk1k1)
      graph.add(nsk2k1)
      graph.remove(nsk2k1)
      check(graph, key2, requireRoot = false, should = false)
      check(graph, key1, requireRoot = false, should = true)
    }
    "support breaking and re-creating chains" in {
      val graph = mkGraph
      graph.add(nsk1k1)
      graph.add(nsk2k1)
      graph.add(nsk3k2)
      check(graph, key2, requireRoot = false, should = true)
      check(graph, key3, requireRoot = false, should = true)
      loggerFactory.assertLogs(graph.remove(nsk2k1), _.warningMessage should include("dangling"))
      check(graph, key2, requireRoot = false, should = false)
      check(graph, key3, requireRoot = false, should = false)
      graph.add(nsk2k1p)
      check(graph, key3, requireRoot = false, should = true)
    }
    "support several chains" in {
      val graph = mkGraph
      graph.add(nsk1k1)
      graph.add(nsk2k1)
      graph.add(nsk3k2)
      check(graph, key3, requireRoot = false, should = true)
      graph.add(nsk3k1)
      check(graph, key3, requireRoot = false, should = true)
      graph.remove(nsk3k1)
      check(graph, key3, requireRoot = false, should = true)
    }

    "deal with cycles" in {
      val graph = mkGraph
      graph.add(nsk1k1)
      graph.add(nsk2k1)
      graph.add(nsk1k2)
      check(graph, key1, requireRoot = false, should = true)
      check(graph, key2, requireRoot = false, should = true)
    }

    "deal with root revocations" in {
      val graph = mkGraph
      graph.add(nsk1k1)
      graph.add(nsk2k1)
      graph.add(nsk3k2)
      graph.remove(nsk1k1)
      check(graph, key1, requireRoot = false, should = false)
      check(graph, key2, requireRoot = false, should = false)
      check(graph, key3, requireRoot = false, should = false)
    }

    "correctly distinguish on root delegations" in {
      val graph = mkGraph
      graph.add(nsk1k1)
      graph.add(nsk3k1)
      check(graph, key1, requireRoot = true, should = true)
      check(graph, key3, requireRoot = true, should = false)
      check(graph, key3, requireRoot = false, should = true)
    }

    "deal with same mappings used twice" in {
      val graph = mkGraph
      graph.add(nsk1k1)
      graph.add(nsk2k1)
      check(graph, key2, requireRoot = true, should = true)
      graph.remove(nsk2k1p)
      // should still be authorized, as the removal used a different element-id
      check(graph, key2, requireRoot = true, should = true)
      // test that random key is not authorized
      check(graph, key3, requireRoot = false, should = false)
      // remove first certificate
      graph.remove(nsk2k1)
      check(graph, key2, requireRoot = true, should = false)
      // add other certificate (we don't remember removes, so we can do that in this test)
      graph.add(nsk2k1p)
      check(graph, key2, requireRoot = true, should = true)
    }

    "burp if we provide a delegation with a wrong namespace" in {
      val graph = mkGraph
      val fakeNs = Namespace(key8.fingerprint)
      val nsk1k1 = mkAuth(NamespaceDelegation(fakeNs, key1, isRootDelegation = true), key1)
      loggerFactory.assertThrowsAndLogs[IllegalArgumentException](
        graph.add(nsk1k1),
        _.errorMessage should include("internal error"),
      )
    }

    "test removal of transactions authorized with different keys" in {
      // can actually do it (add k2 with one key, remove k2 permission with another, but fail to remove it with the other is not valid)
      val graph = mkGraph
      graph.add(nsk1k1)
      graph.add(nsk2k1)
      graph.add(nsk3k2)
      check(graph, key3, requireRoot = true, should = true)

      graph.remove(nsk3k2.copy(transaction = nsk3k2.transaction.update(key = key1)))
      check(graph, key3, requireRoot = true, should = false)
    }

    // tested elsewhere: an authorized transaction is rejected if the signature does not match the content or key
    "under adverse conditions" must {
      "prevent an unauthorized key to authorize an addition" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk3k2) shouldBe false
        check(graph, key3, requireRoot = false, should = false)
      }
      "prevent an unauthorized key to authorize a removal" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        check(graph, key2, requireRoot = false, should = true)
        val fakeRemove = nsk2k1.copy(transaction = nsk2k1.transaction.update(key = key6))
        graph.remove(fakeRemove) shouldBe false
        check(graph, key2, requireRoot = false, should = true)
        graph.remove(nsk2k1)
        check(graph, key2, requireRoot = false, should = false)
      }
      "prevent a non-root authorization to authorize a root authorization" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk3k1)
        check(graph, key3, requireRoot = false, should = true)
        val nsk4k3 = mkAuth(NamespaceDelegation(namespace, key4, isRootDelegation = true), key3)
        graph.add(nsk4k3) shouldBe false
        check(graph, key4, requireRoot = false, should = false)
      }

      "prevent a non-root authorization to authorize a removal" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk3k1)
        graph.add(nsk2k1)
        check(graph, key3, requireRoot = false, should = true)
        check(graph, key2, requireRoot = true, should = true)
        graph.remove(
          nsk2k1.copy(transaction = nsk2k1.transaction.update(key = key3))
        ) shouldBe false
        check(graph, key2, requireRoot = true, should = true)
      }

      "ensure once a delegation is revoked, all depending authorizations will become unauthorized" in {
        val graph = mkGraph
        val nsk4k3 = mkAuth(NamespaceDelegation(namespace, key4, isRootDelegation = true), key3)
        val nsk5k2 = mkAuth(NamespaceDelegation(namespace, key5, isRootDelegation = true), key3)
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        graph.add(nsk3k2)
        graph.add(nsk4k3)
        graph.add(nsk5k2)
        Seq(key3, key4, key5).foreach(check(graph, _, requireRoot = false, should = true))
        loggerFactory.assertLogs(
          {
            graph.remove(nsk2k1)
            Seq(key3, key4, key5).foreach(check(graph, _, requireRoot = false, should = false))
          },
          _.warningMessage should include("The following target keys"),
        )
      }

    }

  }

}
