// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.instances.order.*
import com.digitalasset.canton.topology.Namespace
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.{BaseTestWordSpec, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec

class AuthorizationGraphTest
    extends AnyWordSpec
    with BaseTestWordSpec
    with ProtocolVersionChecksAnyWordSpec
    with BaseAuthorizationGraphTest {

  private def mkGraph: AuthorizationGraph =
    new AuthorizationGraph(namespace, extraDebugInfo = true, loggerFactory)

  import factory.SigningKeys.*

  "authorization graph" when {
    "under normal conditions" should {
      "add simple" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        forAll(allMappings)(check(graph, _, valid = true)(key1))
        forAll(allMappings)(check(graph, _, valid = false)(key2))
      }
      "support longer chains" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        forAll(allMappings)(check(graph, _, valid = true)(key2))
        forAll(allMappings)(check(graph, _, valid = true)(key3))
      }

      "support removal" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.remove(nsk2k1_remove)
        forAll(allMappings)(check(graph, _, valid = false)(key2))
        forAll(allMappings)(check(graph, _, valid = true)(key1))
      }
      "support breaking and re-creating chains" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        forAll(allMappings)(check(graph, _, valid = true)(key2))
        forAll(allMappings)(check(graph, _, valid = true)(key3))
        loggerFactory.assertLogs(
          graph.remove(nsk2k1_remove),
          _.warningMessage should (include regex s"dangling.*${key3.fingerprint}"),
        )
        forAll(allMappings)(check(graph, _, valid = false)(key2))
        forAll(allMappings)(check(graph, _, valid = false)(key3))
        graph.replace(nsk2k1)
        forAll(allMappings)(check(graph, _, valid = true)(key2))
        forAll(allMappings)(check(graph, _, valid = true)(key3))
      }
      "not support several chains" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        forAll(allMappings)(check(graph, _, valid = true)(key3))
        graph.replace(nsk3k1_nonRoot)
        forAll(allButNSD)(check(graph, _, valid = true)(key3))
        check(graph, Code.NamespaceDelegation, valid = false)(key3)
        graph.remove(nsk3k1_nonRoot_remove)
        forAll(allMappings)(check(graph, _, valid = false)(key3))
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
        forAll(allMappings)(check(graph, _, valid = true)(key1))
        forAll(allMappings)(check(graph, _, valid = false)(key2))
        forAll(allMappings)(check(graph, _, valid = false)(key3))
      }

      "deal with root certificate revocations" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)

        val danglingKeys = List(key2, key3).map(_.fingerprint).sorted.mkString(", ")
        loggerFactory.assertLogs(
          graph.remove(nsk1k1_remove),
          _.warningMessage should (include regex s"dangling.*$danglingKeys"),
        )
        forAll(allMappings)(check(graph, _, valid = false)(key1))
        forAll(allMappings)(check(graph, _, valid = false)(key2))
        forAll(allMappings)(check(graph, _, valid = false)(key3))
      }

      "correctly distinguish on delegations that are not allowed to sign a NamespaceDelegation" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk3k1_nonRoot)
        forAll(allMappings)(check(graph, _, valid = true)(key1))
        check(graph, Code.NamespaceDelegation, valid = false)(key3)
        forAll(allButNSD)(check(graph, _, valid = true)(key3))
      }

      "deal with same mappings used twice" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        forAll(allMappings)(check(graph, _, valid = true)(key2))
        // test that random key is not authorized
        forAll(allMappings)(check(graph, _, valid = false)(key3))
        // remove first certificate
        graph.remove(nsk2k1_remove)
        forAll(allMappings)(check(graph, _, valid = false)(key2))
        // add other certificate (we don't remember removes, so we can do that in this test)
        graph.replace(nsk2k1)
        forAll(allMappings)(check(graph, _, valid = true)(key2))
      }

      "reject delegations with a wrong namespace" in {
        val graph = mkGraph
        val fakeNs = Namespace(key8.fingerprint)
        val nsk1k1 = mkAdd(mkNSD(fakeNs, key1, canSignNamespaceDelegations = true), key1)
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
        forAll(allMappings)(check(graph, _, valid = true)(key3))

        graph.remove(replaceSignature(nsk3k2_remove, key1))
        forAll(allMappings)(check(graph, _, valid = false)(key3))
      }
    }

    // tested elsewhere: an authorized transaction is rejected if the signature does not match the content or key
    "under adverse conditions" should {
      "ensure that an unauthorized addition has no effect" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        forAll(allMappings)(check(graph, _, valid = false)(key2))

        loggerFactory.assertLogs(
          graph.replace(nsk3k2),
          _.warningMessage should (include regex s"$namespace are dangling: .*${key3.fingerprint}"),
        )
        forAll(allMappings)(check(graph, _, valid = false)(key3))
      }

      "process an unauthorized removal" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        forAll(allMappings)(check(graph, _, valid = true)(key2))
        val fakeRemove = replaceSignature(nsk2k1_remove, key6)
        forAll(allMappings)(check(graph, _, valid = false)(key6))
        graph.remove(fakeRemove)
        forAll(allMappings)(check(graph, _, valid = false)(key2))
      }

      "ensure that namespace delegations can only be signed by keys with the appropriate delegation restrictions" in {
        val graph = mkGraph
        graph.replace(nsk1k1)
        graph.replace(nsk3k1_nonRoot)
        forAll(allButNSD)(check(graph, _, valid = true)(key3))
        check(graph, Code.NamespaceDelegation, valid = false)(key3)

        // add a delegations signed by k3 via unauthorized add
        loggerFactory.assertLogs(
          graph.replace(Seq(nsk4k3, nsk5k3_nonRoot)),
          _.warningMessage should ((include regex s"$namespace are dangling: .*${key4.fingerprint}") and (include regex s"$namespace are dangling: .*${key5.fingerprint}")),
        )
        forAll(allMappings)(check(graph, _, valid = false)(key4))
        forAll(allMappings)(check(graph, _, valid = false)(key5))
      }

      "update authorizations when downgrading to delegations that cannot sign NSDs" in {
        /* This could happen in the following scenario:
           1. root-cert --k1-> NSD(k2,canSignNSD=true) --k2-> NSD(k3,canSignNSD=true)
           2. downgrade to NSD(k3,canSignNSD=false)
           3. downgrade to NSD(k2,canSignNSD=false)

         */
        val graph = mkGraph
        graph.replace(nsk1k1)
        // first set up the delegations that can sign NSD
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        forAll(allMappings)(check(graph, _, valid = true)(key2))
        forAll(allMappings)(check(graph, _, valid = true)(key3))

        // now downgrade in reverse order
        graph.replace(nsk3k2_nonRoot)
        forAll(allMappings)(check(graph, _, valid = true)(key2))
        // key3 can not sign NSD
        check(graph, Code.NamespaceDelegation, valid = false)(key3)
        // but key3 still can sign all other mappings
        forAll(allButNSD)(check(graph, _, valid = true)(key3))

        loggerFactory.assertLogs(
          // downgrading key2 so that it cannot sign NSD breaks the authorization chain for key3
          graph.replace(nsk2k1_nonRoot),
          _.warningMessage should (include regex s"$namespace are dangling: .*${key3.fingerprint}"),
        )
        // key2 cannot sign NSD
        check(graph, Code.NamespaceDelegation, valid = false)(key2)
        forAll(allButNSD)(check(graph, _, valid = true)(key2))

        // key3 should not be considered authorized anymore at all
        forAll(allMappings)(check(graph, _, valid = false)(key3))
      }

      "ensure once a delegation is revoked, all depending authorizations will become unauthorized" in {
        val graph = mkGraph
        val nsk4k3 = mkAdd(mkNSD(namespace, key4, canSignNamespaceDelegations = true), key3)
        val nsk5k3 = mkAdd(mkNSD(namespace, key5, canSignNamespaceDelegations = true), key3)
        graph.replace(nsk1k1)
        graph.replace(nsk2k1)
        graph.replace(nsk3k2)
        graph.replace(nsk4k3)
        graph.replace(nsk5k3)
        Seq(key3, key4, key5).foreach(key =>
          forAll(allMappings)(check(graph, _, valid = true)(key))
        )
        loggerFactory.assertLogs(
          {
            graph.remove(nsk2k1_remove)
            Seq(key3, key4, key5).foreach(key =>
              forAll(allMappings)(check(graph, _, valid = false)(key))
            )
          },
          _.warningMessage should include("The following target keys"),
        )
      }
    }
  }
}
