// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.topology.transaction.{NamespaceDelegationX, TopologyMappingX}
import com.digitalasset.canton.topology.{Namespace, TestingOwnerWithKeysX}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTestWordSpec, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec

class AuthorizationGraphXTest
    extends AnyWordSpec
    with BaseTestWordSpec
    with ProtocolVersionChecksAnyWordSpec {

  import com.digitalasset.canton.topology.DefaultTestIdentities.*

  private class Fixture {

    val factory = new TestingOwnerWithKeysX(domainManager, loggerFactory, directExecutionContext)
    import factory.SigningKeys.*
    val namespace = Namespace(key1.fingerprint)

    def mkGraph = new AuthorizationGraphX(namespace, extraDebugInfo = true, loggerFactory)

    def mkAuth(
        nsd: NamespaceDelegationX,
        key: SigningPublicKey,
    ): AuthorizedTopologyTransactionX[NamespaceDelegationX] = {
      val tx = factory.mkAdd(nsd, key)
      AuthorizedTopologyTransactionX(tx)
    }

    def mkNs(namespace: Namespace, key: SigningPublicKey, isRootDelegation: Boolean) =
      NamespaceDelegationX.create(namespace, key, isRootDelegation).fold(sys.error, identity)

    val nsk1k1 = mkAuth(mkNs(namespace, key1, isRootDelegation = true), key1)
    val nsk2k1 = mkAuth(mkNs(namespace, key2, isRootDelegation = true), key1)
    val nsk2k1p = mkAuth(mkNs(namespace, key2, isRootDelegation = true), key1)
    val nsk3k2 = mkAuth(mkNs(namespace, key3, isRootDelegation = true), key2)
    val nsk1k2 =
      mkAuth(mkNs(namespace, key1, isRootDelegation = true), key2) // cycle
    val nsk3k1_nonRoot = mkAuth(mkNs(namespace, key3, isRootDelegation = false), key1)

    def replaceSignature[T <: TopologyMappingX](
        authTx: AuthorizedTopologyTransactionX[T],
        key: SigningPublicKey,
    ): AuthorizedTopologyTransactionX[T] = {
      val signature = factory.cryptoApi.crypto.privateCrypto
        .sign(authTx.signedTransaction.transaction.hash.hash, key.fingerprint)
        .value
        .futureValue
        .getOrElse(sys.error(s"Error when signing ${authTx}with $key"))
      authTx.copy(signedTransaction =
        authTx.signedTransaction.copy(signatures = NonEmpty(Set, signature))
      )
    }

  }
  private lazy val fixture = new Fixture()
  private def check(
      graph: AuthorizationGraphX,
      key: SigningPublicKey,
      requireRoot: Boolean,
      valid: Boolean,
  ) = {
    graph.areValidAuthorizationKeys(Set(key.fingerprint), requireRoot = requireRoot) shouldBe valid
  }

  "authorization graph" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet when {
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
        graph.remove(nsk2k1)
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
        loggerFactory.assertLogs(graph.remove(nsk2k1), _.warningMessage should include("dangling"))
        check(graph, key2, requireRoot = false, valid = false)
        check(graph, key3, requireRoot = false, valid = false)
        graph.add(nsk2k1p)
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
        graph.remove(nsk3k1_nonRoot)
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
        graph.remove(nsk1k1)
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
        graph.remove(nsk2k1)
        check(graph, key2, requireRoot = true, valid = false)
        // add other certificate (we don't remember removes, so we can do that in this test)
        graph.add(nsk2k1p)
        check(graph, key2, requireRoot = true, valid = true)
      }

      "reject delegations with a wrong namespace" in {
        val graph = mkGraph
        val fakeNs = Namespace(key8.fingerprint)
        val nsk1k1 = mkAuth(mkNs(fakeNs, key1, isRootDelegation = true), key1)
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

        graph.remove(replaceSignature(nsk3k2, key1))
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
        val fakeRemove = replaceSignature(nsk2k1, key6)
        graph.remove(fakeRemove) shouldBe false
        check(graph, key2, requireRoot = false, valid = true)
        graph.remove(nsk2k1)
        check(graph, key2, requireRoot = false, valid = false)
      }
      "prevent a non-root authorization to authorize a root authorization" in {
        val graph = mkGraph
        graph.add(nsk1k1)
        graph.add(nsk3k1_nonRoot)
        check(graph, key3, requireRoot = false, valid = true)
        val nsk4k3 = mkAuth(mkNs(namespace, key4, isRootDelegation = true), key3)
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
        graph.remove(replaceSignature(nsk2k1, key3)) shouldBe false
        check(graph, key2, requireRoot = true, valid = true)
      }

      "ensure once a delegation is revoked, all depending authorizations will become unauthorized" in {
        val graph = mkGraph
        val nsk4k3 = mkAuth(mkNs(namespace, key4, isRootDelegation = true), key3)
        val nsk5k2 = mkAuth(mkNs(namespace, key5, isRootDelegation = true), key3)
        graph.add(nsk1k1)
        graph.add(nsk2k1)
        graph.add(nsk3k2)
        graph.add(nsk4k3)
        graph.add(nsk5k2)
        Seq(key3, key4, key5).foreach(check(graph, _, requireRoot = false, valid = true))
        loggerFactory.assertLogs(
          {
            graph.remove(nsk2k1)
            Seq(key3, key4, key5).foreach(check(graph, _, requireRoot = false, valid = false))
          },
          _.warningMessage should include("The following target keys"),
        )
      }
    }
  }
}
