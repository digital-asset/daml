// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.instances.order.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.AuthorizedNamespaceDelegation
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinition,
  NamespaceDelegation,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{Namespace, TestingOwnerWithKeys}
import com.digitalasset.canton.{BaseTestWordSpec, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec

class DecentralizedNamespaceAuthorizationGraphTest
    extends AnyWordSpec
    with BaseTestWordSpec
    with ProtocolVersionChecksAnyWordSpec {

  import com.digitalasset.canton.topology.DefaultTestIdentities.*

  private class Fixture {

    val factory = new TestingOwnerWithKeys(sequencerId, loggerFactory, directExecutionContext)

    import factory.SigningKeys.*

    val decentralizedNamespace =
      Namespace(Fingerprint.tryCreate("decentralized-namespace-fingerprint"))
    val ns1 = Namespace(key1.fingerprint)
    val ns2 = Namespace(key2.fingerprint)
    val ns3 = Namespace(key3.fingerprint)
    val owners = NonEmpty(Set, ns1, ns2, ns3)
    val decentralizedNamespaceDefinition =
      DecentralizedNamespaceDefinition
        .create(decentralizedNamespace, PositiveInt.two, owners)
        .fold(sys.error, identity)

    def mkGraph =
      DecentralizedNamespaceAuthorizationGraph(
        decentralizedNamespaceDefinition,
        owners
          .map(new AuthorizationGraph(_, extraDebugInfo = false, loggerFactory = loggerFactory))
          .forgetNE
          .toSeq,
      )

    implicit class DecentralizedNamespaceAuthorizationGraphExtension(
        dns: DecentralizedNamespaceAuthorizationGraph
    ) {
      def addAuth(authorizedNSD: AuthorizedNamespaceDelegation): Unit =
        dns.ownerGraphs
          .find(_.namespace == authorizedNSD.mapping.namespace)
          .foreach(_.replace(authorizedNSD))

      def removeAuth(authorizedNSD: AuthorizedNamespaceDelegation): Unit =
        dns.ownerGraphs
          .find(_.namespace == authorizedNSD.mapping.namespace)
          .foreach(_.remove(authorizedNSD))
    }

    def mkAdd(
        nsd: NamespaceDelegation,
        keys: SigningPublicKey*
    ): AuthorizedTopologyTransaction[NamespaceDelegation] = {
      val tx = factory.mkAddMultiKey(
        nsd,
        NonEmpty.from(keys.toSet).getOrElse(sys.error("no signing keys specified")),
      )
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

    val ns1k1k1 = mkAdd(mkNs(ns1, key1, isRootDelegation = true), key1)

    val ns2k2k2 = mkAdd(mkNs(ns2, key2, isRootDelegation = true), key2)
    val ns2k2k2_remove = mkRemove(mkNs(ns2, key2, isRootDelegation = true), key2)
    val ns2k5k2 = mkAdd(mkNs(ns2, key5, isRootDelegation = true), key2)
    val ns2k5k2_remove = mkRemove(mkNs(ns2, key5, isRootDelegation = true), key2)
    val ns2k5k8 = mkAdd(mkNs(ns2, key5, isRootDelegation = true), key8)
    val ns2k2k5 = mkAdd(mkNs(ns2, key2, isRootDelegation = true), key5)
    val ns2k8k5 = mkAdd(mkNs(ns2, key8, isRootDelegation = true), key5)
    val ns2k8k5_remove = mkRemove(mkNs(ns2, key8, isRootDelegation = true), key5)
    val ns2k8k2_nonRoot = mkAdd(mkNs(ns2, key8, isRootDelegation = false), key2)
    val ns2k8k2_nonRoot_remove = mkRemove(mkNs(ns2, key8, isRootDelegation = false), key2)

    val ns3k3k3 = mkAdd(mkNs(ns3, key3, isRootDelegation = true), key3)

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
        .futureValueUS
        .getOrElse(sys.error(s"Error when signing ${authTx}with $key"))
      authTx.copy(transaction = authTx.transaction.copy(signatures = NonEmpty(Set, signature)))
    }

  }

  private lazy val fixture = new Fixture()

  private def check(
      graph: AuthorizationCheck,
      requireRoot: Boolean,
      valid: Boolean,
  )(keys: SigningPublicKey*) =
    graph.existsAuthorizedKeyIn(
      keys.map(_.fingerprint).toSet,
      requireRoot = requireRoot,
    ) shouldBe valid

  "authorization graph for a decentralized namespace" when {

    "only having namespace delegations for its constituents" should {
      import fixture.*
      import fixture.factory.SigningKeys.*
      "work for a simple quorum" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns3k3k3)

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

        graph.removeAuth(ns2k2k2_remove)
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
          graph.removeAuth(ns2k5k2_remove),
          _.warningMessage should (include regex s"dangling.*${key8.fingerprint}"),
        )
        check(graph, requireRoot = false, valid = false)(key1, key5)
        check(graph, requireRoot = false, valid = false)(key1, key8)
        graph.addAuth(ns2k5k2)
        check(graph, requireRoot = false, valid = true)(key1, key5)
        check(graph, requireRoot = false, valid = true)(key1, key8)
      }

      "not support several chains" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)

        graph.addAuth(ns2k5k2)
        // this is ns2k8 serial=1
        graph.addAuth(ns2k8k5)
        check(graph, requireRoot = false, valid = true)(key1, key8)

        // this is ns2k8 serial=2 and overwrites the previous mapping ns2k8 signed by k5
        graph.addAuth(ns2k8k2_nonRoot)
        check(graph, requireRoot = false, valid = true)(key1, key8)

        // this is ns2k8 serial=3 and removes the ns2k8 mapping entirely
        graph.removeAuth(ns2k8k2_nonRoot_remove)
        check(graph, requireRoot = false, valid = false)(key1, key8)
      }

      "deal with cycles" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)

        val danglingKeys = List(key5, key8).map(_.fingerprint).sorted.mkString(", ")
        loggerFactory.assertLogs(
          // this overwrites ns2k5k2, leading to a break in the authorization chain for the now dangling k5 and k8
          graph.addAuth(ns2k5k8),
          _.warningMessage should (include regex s"dangling.*$danglingKeys"),
        )
        check(graph, requireRoot = false, valid = true)(key1, key2)
        check(graph, requireRoot = false, valid = false)(key1, key5)
        check(graph, requireRoot = false, valid = false)(key1, key8)
      }

      "deal with root revocations" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns3k3k3)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)

        val danglingKeys = List(key5, key8).map(_.fingerprint).sorted.mkString(", ")
        loggerFactory.assertLogs(
          graph.removeAuth(ns2k2k2_remove),
          _.warningMessage should (include regex s"dangling.*$danglingKeys"),
        )
        check(graph, requireRoot = false, valid = false)(key1, key2)
        check(graph, requireRoot = false, valid = false)(key1, key5)
        check(graph, requireRoot = false, valid = false)(key1, key8)

        // however, key1 and key3 can still reach quorum
        check(graph, requireRoot = false, valid = true)(key1, key3)
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
        graph.removeAuth(ns2k5k2_remove)
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

        graph.removeAuth(replaceSignature(ns2k8k5_remove, key2))
        check(graph, requireRoot = true, valid = false)(key1, key8)
      }
    }
  }
}
