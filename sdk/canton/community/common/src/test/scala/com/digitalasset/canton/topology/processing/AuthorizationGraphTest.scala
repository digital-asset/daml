// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.instances.order.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{SigningKeyUsage, SigningPublicKey}
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
    val nsk2k3 = mkAdd(mkNs(namespace, key2, isRootDelegation = true), key3)
    val nsk2k1_remove = mkRemove(mkNs(namespace, key2, isRootDelegation = true), key1)
    val nsk2k1_nonRoot = mkAdd(mkNs(namespace, key2, isRootDelegation = false), key1)
    val nsk3k2 = mkAdd(mkNs(namespace, key3, isRootDelegation = true), key2)
    val nsk3k2_remove = mkRemove(mkNs(namespace, key3, isRootDelegation = true), key2)
    val nsk3k2_nonRoot = mkAdd(mkNs(namespace, key3, isRootDelegation = false), key2)
    val nsk1k2 =
      mkAdd(mkNs(namespace, key1, isRootDelegation = true), key2) // cycle
    val nsk3k1_nonRoot = mkAdd(mkNs(namespace, key3, isRootDelegation = false), key1)
    val nsk3k1_nonRoot_remove = mkRemove(mkNs(namespace, key3, isRootDelegation = false), key1)
    val nsk4k3 = mkAdd(mkNs(namespace, key4, isRootDelegation = true), key3)
    val nsk5k3_nonRoot = mkAdd(mkNs(namespace, key5, isRootDelegation = false), key3)

    def replaceSignature[T <: TopologyMapping](
        authTx: AuthorizedTopologyTransaction[T],
        key: SigningPublicKey,
    ): AuthorizedTopologyTransaction[T] = {
      // in this test we only sign namespace delegations so we can limit the usage to NamespaceOnly
      val signature = factory.cryptoApi.crypto.privateCrypto
        .sign(
          authTx.hash.hash,
          key.fingerprint,
          SigningKeyUsage.NamespaceOnly,
        )
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
  ) =
    graph.existsAuthorizedKeyIn(Set(key.fingerprint), requireRoot = requireRoot) shouldBe valid

  "authorization graph" when {
    import fixture.*
    import fixture.factory.SigningKeys.*

    "under normal conditions" should {
      "add simple" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        check(graph, key1, requireRoot = true, valid = true)
        check(graph, key1, requireRoot = false, valid = true)
        check(graph, key2, requireRoot = false, valid = false)
      }
      "support longer chains" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        check(graph, key2, requireRoot = false, valid = true)
        check(graph, key3, requireRoot = false, valid = true)
      }

      "support removal" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.remove(nsk2k1_remove)
        check(graph, key2, requireRoot = false, valid = false)
        check(graph, key1, requireRoot = false, valid = true)
      }
      "support breaking and re-creating chains" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        check(graph, key2, requireRoot = false, valid = true)
        check(graph, key3, requireRoot = false, valid = true)
        loggerFactory.assertLogs(
          graph.remove(nsk2k1_remove),
          _.warningMessage should (include regex s"dangling.*${key3.fingerprint}"),
        )
        check(graph, key2, requireRoot = false, valid = false)
        check(graph, key3, requireRoot = false, valid = false)
        graph.replace(nsk2k1)
        check(graph, key3, requireRoot = false, valid = true)
      }
      "not support several chains" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        check(graph, key3, requireRoot = false, valid = true)
        graph.replace(nsk3k1_nonRoot)
        check(graph, key3, requireRoot = false, valid = true)
        graph.remove(nsk3k1_nonRoot_remove)
        check(graph, key3, requireRoot = false, valid = false)
      }

      "deal with cycles" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)

        val danglingKeys = List(key2, key3).map(_.fingerprint).sorted.mkString(", ")
        loggerFactory.assertLogs(
          // this overwrites nsk2k1, leading to a break in the authorization chain for the now dangling k2 and k3
          graph.replace(nsk2k3),
          _.warningMessage should (include regex s"dangling.*$danglingKeys"),
        )
        check(graph, key1, requireRoot = false, valid = true)
        check(graph, key2, requireRoot = false, valid = false)
        check(graph, key3, requireRoot = false, valid = false)
      }

      "deal with root revocations" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)

        val danglingKeys = List(key2, key3).map(_.fingerprint).sorted.mkString(", ")
        loggerFactory.assertLogs(
          graph.remove(nsk1k1_remove),
          _.warningMessage should (include regex s"dangling.*$danglingKeys"),
        )
        check(graph, key1, requireRoot = false, valid = false)
        check(graph, key2, requireRoot = false, valid = false)
        check(graph, key3, requireRoot = false, valid = false)
      }

      "correctly distinguish on root delegations" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk3k1_nonRoot)
        check(graph, key1, requireRoot = true, valid = true)
        check(graph, key3, requireRoot = true, valid = false)
        check(graph, key3, requireRoot = false, valid = true)
      }

      "deal with same mappings used twice" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        check(graph, key2, requireRoot = true, valid = true)
        // test that random key is not authorized
        check(graph, key3, requireRoot = false, valid = false)
        // remove first certificate
        graph.remove(nsk2k1_remove)
        check(graph, key2, requireRoot = true, valid = false)
        // add other certificate (we don't remember removes, so we can do that in this test)
        graph.replace(nsk2k1)
        check(graph, key2, requireRoot = true, valid = true)
      }

      "reject delegations with a wrong namespace" in {
        val graph = mkGraph
        val fakeNs = Namespace(key8.fingerprint)
        val nsk1k1 = mkAdd(mkNs(fakeNs, key1, isRootDelegation = true), key1)
        loggerFactory.assertThrowsAndLogs[IllegalArgumentException](
          graph.replace(nsk1k1),
          _.errorMessage should include("internal error"),
        )
      }

      "test removal of transactions authorized with different keys" in {
        // can actually do it (add k2 with one key, remove k2 permission with another, but fail to remove it with the other is not valid)
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        check(graph, key3, requireRoot = true, valid = true)

        graph.remove(replaceSignature(nsk3k2_remove, key1))
        check(graph, key3, requireRoot = true, valid = false)
      }
    }

    // tested elsewhere: an authorized transaction is rejected if the signature does not match the content or key
    "under adverse conditions" should {
      "ensure that an unauthorized addition has no effect" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        check(graph, key2, requireRoot = false, valid = false)

        loggerFactory.assertLogs(
          graph.replace(nsk3k2),
          _.warningMessage should (include regex s"$namespace are dangling: .*${key3.fingerprint}"),
        )
        check(graph, key3, requireRoot = false, valid = false)
      }

      "process an unauthorized removal" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        check(graph, key2, requireRoot = true, valid = true)
        val fakeRemove = replaceSignature(nsk2k1_remove, key6)
        check(graph, key6, requireRoot = false, valid = false)
        graph.remove(fakeRemove)
        check(graph, key2, requireRoot = false, valid = false)
      }

      "ensure that a non-root authorization does not authorize other namespace delegations" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk3k1_nonRoot)
        check(graph, key3, requireRoot = false, valid = true)
        check(graph, key3, requireRoot = true, valid = false)

        // add a root delegation signed by k3 via unauthorized add
        loggerFactory.assertLogs(
          graph.replace(Seq(nsk4k3, nsk5k3_nonRoot)),
          _.warningMessage should ((include regex s"$namespace are dangling: .*${key4.fingerprint}") and (include regex s"$namespace are dangling: .*${key5.fingerprint}")),
        )
        check(graph, key4, requireRoot = false, valid = false)
        check(graph, key5, requireRoot = false, valid = false)
      }

      "update authorizations when downgrading to non-root delegations" in {
        /* This could happen in the following scenario:
           1. root -k1-> NSD(k2,root=true) -k2-> NSD(k3,root=true)
           2. downgrade to NSD(k3,root=false)
           3. downgrade to NSD(k2,root=false)

         */
        val graph = mkGraph
        graph.replace(nsk1k1)
        // first set up the root delegations and verify that they work
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        check(graph, key2, requireRoot = true, valid = true)
        check(graph, key3, requireRoot = true, valid = true)

        // now downgrade in reverse order
        graph.replace(nsk3k2_nonRoot)
        check(graph, key2, requireRoot = true, valid = true)
        // key3 still has a non-root delegation
        check(graph, key3, requireRoot = false, valid = true)
        // but it's not a root delegation
        check(graph, key3, requireRoot = true, valid = false)

        loggerFactory.assertLogs(
          // downgrading key2 to a non-root delegation breaks the authorization chain for key3
          graph.replace(nsk2k1_nonRoot),
          _.warningMessage should (include regex s"$namespace are dangling: .*${key3.fingerprint}"),
        )
        // key2 only has a non-root delegation
        check(graph, key2, requireRoot = true, valid = false)
        check(graph, key2, requireRoot = false, valid = true)

        // key3 should not be considered authorized anymore at all
        check(graph, key3, requireRoot = true, valid = false)
        check(graph, key3, requireRoot = false, valid = false)
      }

      "ensure once a delegation is revoked, all depending authorizations will become unauthorized" in {
        val graph = mkGraph
        val nsk4k3 = mkAdd(mkNs(namespace, key4, isRootDelegation = true), key3)
        val nsk5k3 = mkAdd(mkNs(namespace, key5, isRootDelegation = true), key3)
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        graph.replace(nsk4k3)
        graph.replace(nsk5k3)
        Seq(key3, key4, key5).foreach(check(graph, _, requireRoot = true, valid = true))
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
