// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, SigningPublicKey}
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransactionX.AuthorizedNamespaceDelegationX
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinitionX,
  NamespaceDelegationX,
  TopologyMappingX,
}
import com.digitalasset.canton.topology.{Namespace, TestingOwnerWithKeysX}
import com.digitalasset.canton.{BaseTestWordSpec, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec

class DecentralizedNamespaceAuthorizationGraphXTest
    extends AnyWordSpec
    with BaseTestWordSpec
    with ProtocolVersionChecksAnyWordSpec {

  import com.digitalasset.canton.topology.DefaultTestIdentities.*

  private class Fixture {

    val factory = new TestingOwnerWithKeysX(domainManager, loggerFactory, directExecutionContext)

    import factory.SigningKeys.*

    val decentralizedNamespace =
      Namespace(Fingerprint.tryCreate("decentralized-namespace-fingerprint"))
    val ns1 = Namespace(key1.fingerprint)
    val ns2 = Namespace(key2.fingerprint)
    val ns3 = Namespace(key3.fingerprint)
    val owners = NonEmpty(Set, ns1, ns2, ns3)
    val decentralizedNamespaceDefinition =
      DecentralizedNamespaceDefinitionX
        .create(decentralizedNamespace, PositiveInt.two, owners)
        .fold(sys.error, identity)

    def mkGraph =
      DecentralizedNamespaceAuthorizationGraphX(
        decentralizedNamespaceDefinition,
        new AuthorizationGraphX(
          decentralizedNamespace,
          extraDebugInfo = false,
          loggerFactory = loggerFactory,
        ),
        owners
          .map(new AuthorizationGraphX(_, extraDebugInfo = false, loggerFactory = loggerFactory))
          .forgetNE
          .toSeq,
      )

    implicit class DecentralizedNamespaceAuthorizationGraphXExtension(
        dns: DecentralizedNamespaceAuthorizationGraphX
    ) {
      def addAuth(authorizedNSD: AuthorizedNamespaceDelegationX) = {
        val found = (dns.direct +: dns.ownerGraphs)
          .find(_.namespace == authorizedNSD.mapping.namespace)
        found.exists(_.add(authorizedNSD))
      }

      def removeAuth(authorizedNSD: AuthorizedNamespaceDelegationX) =
        (dns.direct +: dns.ownerGraphs)
          .find(_.namespace == authorizedNSD.mapping.namespace)
          .exists(_.remove(authorizedNSD))

    }

    def mkAuth(
        nsd: NamespaceDelegationX,
        key: SigningPublicKey,
    ): AuthorizedTopologyTransactionX[NamespaceDelegationX] = {
      val tx = factory.mkAdd(nsd, key)
      AuthorizedTopologyTransactionX(tx)
    }

    def mkNs(namespace: Namespace, key: SigningPublicKey, isRootDelegation: Boolean) =
      NamespaceDelegationX.tryCreate(namespace, key, isRootDelegation)

    val ns1k1k1 = mkAuth(mkNs(ns1, key1, isRootDelegation = true), key1)
    val ns1k4k1 = mkAuth(mkNs(ns1, key4, isRootDelegation = true), key1)

    val ns2k2k2 = mkAuth(mkNs(ns2, key2, isRootDelegation = true), key2)
    val ns2k5k2 = mkAuth(mkNs(ns2, key5, isRootDelegation = true), key2)
    val ns2k2k5 = mkAuth(mkNs(ns2, key5, isRootDelegation = true), key2)
    val ns2k8k5 = mkAuth(mkNs(ns2, key8, isRootDelegation = true), key5)
    val ns2k8k2_nonRoot = mkAuth(mkNs(ns2, key8, isRootDelegation = false), key2)

    val ns3k3k3 = mkAuth(mkNs(ns3, key3, isRootDelegation = true), key3)
    val ns3k6k3 = mkAuth(mkNs(ns3, key6, isRootDelegation = true), key3)

    def replaceSignature[T <: TopologyMappingX](
        authTx: AuthorizedTopologyTransactionX[T],
        key: SigningPublicKey,
    ): AuthorizedTopologyTransactionX[T] = {
      val signature = factory.cryptoApi.crypto.privateCrypto
        .sign(authTx.hash.hash, key.fingerprint)
        .value
        .futureValue
        .getOrElse(sys.error(s"Error when signing ${authTx}with $key"))
      authTx.copy(transaction = authTx.transaction.copy(signatures = NonEmpty(Set, signature)))
    }

  }

  private lazy val fixture = new Fixture()

  private def check(
      graph: AuthorizationCheckX,
      requireRoot: Boolean,
      valid: Boolean,
  )(keys: SigningPublicKey*) = {
    graph.areValidAuthorizationKeys(
      keys.map(_.fingerprint).toSet,
      requireRoot = requireRoot,
    ) shouldBe valid
  }

  "authorization graph for a decentralized namespace" when {

    "only having namespace delegations for its constituents" should {
      import fixture.*
      import fixture.factory.SigningKeys.*
      "work for a simple quorum" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1) shouldBe true
        graph.addAuth(ns2k2k2) shouldBe true
        graph.addAuth(ns3k3k3) shouldBe true

        // Individual keys are not enough
        for {
          key <- Seq(key1, key2, key3)
          requireRoot <- Seq(true, false)
        } {
          check(graph, requireRoot, valid = false)(key)
        }

        // at least quorum number of signatures is enough
        Seq(key1, key2, key3)
          .combinations(decentralizedNamespaceDefinition.threshold.value)
          .foreach { keys =>
            check(graph, requireRoot = false, valid = true)(keys*)
            check(graph, requireRoot = true, valid = true)(keys*)
          }
      }
      "support longer chains" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns3k3k3)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)
        check(graph, requireRoot = false, valid = true)(key1, key8, key3)
      }

      "support removal" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns3k3k3)

        graph.removeAuth(ns2k2k2)
        check(graph, requireRoot = false, valid = false)(key1, key2)
        check(graph, requireRoot = false, valid = true)(key1, key3)
      }

      "support breaking and re-creating chains" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)
        check(graph, requireRoot = false, valid = true)(key1, key2)
        check(graph, requireRoot = false, valid = true)(key1, key5)
        check(graph, requireRoot = false, valid = true)(key1, key8)
        loggerFactory.assertLogs(
          graph.removeAuth(ns2k5k2),
          _.warningMessage should include("dangling"),
        )
        check(graph, requireRoot = false, valid = false)(key1, key5)
        check(graph, requireRoot = false, valid = false)(key1, key8)
        graph.addAuth(ns2k5k2)
        check(graph, requireRoot = false, valid = true)(key1, key5)
        check(graph, requireRoot = false, valid = true)(key1, key8)
      }

      "support several chains" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)
        check(graph, requireRoot = false, valid = true)(key1, key8)
        graph.addAuth(ns2k8k2_nonRoot)
        check(graph, requireRoot = false, valid = true)(key1, key8)
        graph.removeAuth(ns2k8k2_nonRoot)
        check(graph, requireRoot = false, valid = true)(key1, key8)
      }

      "deal with cycles" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k2k5)
        check(graph, requireRoot = false, valid = true)(key1, key2)
        check(graph, requireRoot = false, valid = true)(key1, key5)
      }

      "deal with root revocations" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)
        graph.removeAuth(ns2k2k2)
        check(graph, requireRoot = false, valid = false)(key1, key2)
        check(graph, requireRoot = false, valid = false)(key1, key5)
        check(graph, requireRoot = false, valid = false)(key1, key8)
      }

      "correctly distinguish on root delegations" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns2k8k2_nonRoot)
        check(graph, requireRoot = true, valid = true)(key1, key2)
        check(graph, requireRoot = true, valid = false)(key1, key8)
        check(graph, requireRoot = false, valid = true)(key1, key8)
      }

      "deal with same mappings used twice" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns2k5k2)
        check(graph, requireRoot = true, valid = true)(key1, key5)
        // test that random key is not authorized
        check(graph, requireRoot = false, valid = false)(key1, key3)
        // remove first certificate
        graph.removeAuth(ns2k5k2)
        check(graph, requireRoot = true, valid = false)(key1, key5)
        // add other certificate (we don't remember removes, so we can do that in this test)
        graph.addAuth(ns2k5k2)
        check(graph, requireRoot = true, valid = true)(key1, key5)
      }

      "test removal of transactions authorized with different keys" in {
        // can actually do it (add k2 with one key, remove k2 permission with another, but fail to remove it with the other is not valid)
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)
        check(graph, requireRoot = true, valid = true)(key1, key8)

        graph.removeAuth(replaceSignature(ns2k8k5, key2))
        check(graph, requireRoot = true, valid = false)(key1, key8)
      }
    }
  }

  // TODO(#12390) add test that checks that a namespace delegation for the decentralized namespace gets invalidated if
  //              one of the keys signing that NSD gets invalidated
}
