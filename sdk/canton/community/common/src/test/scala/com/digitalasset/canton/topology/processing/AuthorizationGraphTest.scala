// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.topology.transaction.{NamespaceDelegation, TopologyMapping}
import com.digitalasset.canton.topology.{Namespace, TestingOwnerWithKeys}
import com.digitalasset.canton.{BaseTestWordSpec, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec

class AuthorizationGraphTest
    extends AnyWordSpec
    with BaseTestWordSpec
    with ProtocolVersionChecksAnyWordSpec {

  import com.digitalasset.canton.topology.DefaultTestIdentities.*

  private class Fixture {

    val factory = new TestingOwnerWithKeys(sequencerId, loggerFactory, directExecutionContext)
    import factory.SigningKeys.*
    val namespace = Namespace(key1.fingerprint)

    def mkGraph = new AuthorizationGraph(namespace, extraDebugInfo = true, loggerFactory)

    def mkAdd(
        nsd: NamespaceDelegation,
        key: SigningPublicKey,
    ): AuthorizedTopologyTransaction[NamespaceDelegation] = {
      val tx = factory.mkAdd(nsd, key)
      AuthorizedTopologyTransaction(tx)
    }

    def mkRemove(
        nsd: NamespaceDelegation,
        key: SigningPublicKey,
    ): AuthorizedTopologyTransaction[NamespaceDelegation] = {
      val tx = factory.mkRemove(nsd, NonEmpty(Set, key), PositiveInt.two)
      AuthorizedTopologyTransaction(tx)
    }

    def mkNs(namespace: Namespace, key: SigningPublicKey, isRootDelegation: Boolean) =
      NamespaceDelegation.tryCreate(namespace, key, isRootDelegation)

    val nsk1k1 = mkAdd(mkNs(namespace, key1, isRootDelegation = true), key1)
    val nsk1k1_remove = mkRemove(mkNs(namespace, key1, isRootDelegation = true), key1)
    val nsk2k1 = mkAdd(mkNs(namespace, key2, isRootDelegation = true), key1)
    val nsk2k1_remove = mkRemove(mkNs(namespace, key2, isRootDelegation = true), key1)
    val nsk3k2 = mkAdd(mkNs(namespace, key3, isRootDelegation = true), key2)
    val nsk3k2_remove = mkRemove(mkNs(namespace, key3, isRootDelegation = true), key2)
    val nsk1k2 =
      mkAdd(mkNs(namespace, key1, isRootDelegation = true), key2) // cycle
    val nsk3k1_nonRoot = mkAdd(mkNs(namespace, key3, isRootDelegation = false), key1)
    val nsk3k1_nonRoot_remove = mkRemove(mkNs(namespace, key3, isRootDelegation = false), key1)

    def replaceSignature[T <: TopologyMapping](
        authTx: AuthorizedTopologyTransaction[T],
        key: SigningPublicKey,
    ): AuthorizedTopologyTransaction[T] = {
      val signature = factory.cryptoApi.crypto.privateCrypto
        .sign(authTx.hash.hash, key.fingerprint)
        .value
        .failOnShutdown
        .futureValue
        .getOrElse(sys.error(s"Error when signing ${authTx}with $key"))
      authTx.copy(transaction = authTx.transaction.copy(signatures = NonEmpty(Set, signature)))
    }

  }
  private lazy val fixture = new Fixture()
  private def check(
      graph: AuthorizationGraph,
      key: SigningPublicKey,
      requireRoot: Boolean,
      valid: Boolean,
  ) = {
    graph.existsAuthorizedKeyIn(Set(key.fingerprint), requireRoot = requireRoot) shouldBe valid
  }

  "authorization graph" when {
    import fixture.*
    import fixture.factory.SigningKeys.*

    "under normal conditions" should {
      "add simple" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        check(graph, key1, requireRoot = true, valid = true)
        check(graph, key1, requireRoot = false, valid = true)
        check(graph, key2, requireRoot = false, valid = false)
      }
      "support longer chains" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        graph.add(nsk3k2)
        check(graph, key2, requireRoot = false, valid = true)
        check(graph, key3, requireRoot = false, valid = true)
      }

      "support removal" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        graph.remove(nsk2k1_remove)
        check(graph, key2, requireRoot = false, valid = false)
        check(graph, key1, requireRoot = false, valid = true)
      }
      "support breaking and re-creating chains" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        graph.add(nsk3k2)
        check(graph, key2, requireRoot = false, valid = true)
        check(graph, key3, requireRoot = false, valid = true)
        loggerFactory.assertLogs(
          graph.remove(nsk2k1_remove),
          _.warningMessage should include("dangling"),
        )
        check(graph, key2, requireRoot = false, valid = false)
        check(graph, key3, requireRoot = false, valid = false)
        graph.add(nsk2k1)
        check(graph, key3, requireRoot = false, valid = true)
      }
      "support several chains" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        graph.add(nsk3k2)
        check(graph, key3, requireRoot = false, valid = true)
        graph.add(nsk3k1_nonRoot)
        check(graph, key3, requireRoot = false, valid = true)
        graph.remove(nsk3k1_nonRoot_remove)
        check(graph, key3, requireRoot = false, valid = true)
      }

      "deal with cycles" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        graph.add(nsk1k2)
        check(graph, key1, requireRoot = false, valid = true)
        check(graph, key2, requireRoot = false, valid = true)
      }

      "deal with root revocations" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        graph.add(nsk3k2)
        graph.remove(nsk1k1_remove)
        check(graph, key1, requireRoot = false, valid = false)
        check(graph, key2, requireRoot = false, valid = false)
        check(graph, key3, requireRoot = false, valid = false)
      }

      "correctly distinguish on root delegations" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk3k1_nonRoot)
        check(graph, key1, requireRoot = true, valid = true)
        check(graph, key3, requireRoot = true, valid = false)
        check(graph, key3, requireRoot = false, valid = true)
      }

      "deal with same mappings used twice" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        check(graph, key2, requireRoot = true, valid = true)
        // test that random key is not authorized
        check(graph, key3, requireRoot = false, valid = false)
        // remove first certificate
        graph.remove(nsk2k1_remove)
        check(graph, key2, requireRoot = true, valid = false)
        // add other certificate (we don't remember removes, so we can do that in this test)
        graph.add(nsk2k1)
        check(graph, key2, requireRoot = true, valid = true)
      }

      "reject delegations with a wrong namespace" in {
        val graph = mkGraph
        val fakeNs = Namespace(key8.fingerprint)
        val nsk1k1 = mkAdd(mkNs(fakeNs, key1, isRootDelegation = true), key1)
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
        check(graph, key3, requireRoot = true, valid = true)

        graph.remove(replaceSignature(nsk3k2_remove, key1))
        check(graph, key3, requireRoot = true, valid = false)
      }
    }

    // tested elsewhere: an authorized transaction is rejected if the signature does not match the content or key
    "under adverse conditions" should {
      "prevent an unauthorized key to authorize an addition" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk3k2) shouldBe false
        check(graph, key3, requireRoot = false, valid = false)
      }
      "prevent an unauthorized key to authorize a removal" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        check(graph, key2, requireRoot = false, valid = true)
        val fakeRemove = replaceSignature(nsk2k1_remove, key6)
        graph.remove(fakeRemove) shouldBe false
        check(graph, key2, requireRoot = false, valid = true)
        graph.remove(nsk2k1_remove)
        check(graph, key2, requireRoot = false, valid = false)
      }
      "prevent a non-root authorization to authorize a root authorization" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk3k1_nonRoot)
        check(graph, key3, requireRoot = false, valid = true)
        val nsk4k3 = mkAdd(mkNs(namespace, key4, isRootDelegation = true), key3)
        graph.add(nsk4k3) shouldBe false
        check(graph, key4, requireRoot = false, valid = false)
      }

      "prevent a non-root authorization to authorize a removal" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk3k1_nonRoot)
        graph.add(nsk2k1)
        check(graph, key3, requireRoot = false, valid = true)
        check(graph, key2, requireRoot = true, valid = true)
        graph.remove(replaceSignature(nsk2k1_remove, key3)) shouldBe false
        check(graph, key2, requireRoot = true, valid = true)
      }

      "ensure once a delegation is revoked, all depending authorizations will become unauthorized" in {
        val graph = mkGraph
        val nsk4k3 = mkAdd(mkNs(namespace, key4, isRootDelegation = true), key3)
        val nsk5k2 = mkAdd(mkNs(namespace, key5, isRootDelegation = true), key3)
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        graph.add(nsk3k2)
        graph.add(nsk4k3)
        graph.add(nsk5k2)
        Seq(key3, key4, key5).foreach(check(graph, _, requireRoot = false, valid = true))
        loggerFactory.assertLogs(
          {
            graph.remove(nsk2k1_remove)
            Seq(key3, key4, key5).foreach(check(graph, _, requireRoot = false, valid = false))
          },
          _.warningMessage should include("The following target keys"),
        )
      }
    }
  }
}
