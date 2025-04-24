// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.topology.DefaultTestIdentities.sequencerId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
  CanSignSpecificMappings,
}
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinition,
  DelegationRestriction,
  NamespaceDelegation,
  SingleTransactionSignature,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{Namespace, TestingOwnerWithKeys}
import org.scalatest.Assertion

import scala.util.Random

trait BaseAuthorizationGraphTest { self: BaseTest =>

  private val seed = Random.nextLong()
  private val random = new Random(seed)
  logger.debug(s"Running ${this.getClass} with seed $seed")

  val factory = new TestingOwnerWithKeys(sequencerId, loggerFactory, directExecutionContext)
  import factory.SigningKeys.*

  val allMappings = TopologyMapping.Code.all.toSet
  val allButNSD = allMappings - TopologyMapping.Code.NamespaceDelegation

  val namespace = Namespace(key1.fingerprint)
  val nsk1k1 = mkAdd(mkNSD(namespace, key1, canSignNamespaceDelegations = true), key1)
  val nsk1k1_remove = mkRemove(mkNSD(namespace, key1, canSignNamespaceDelegations = true), key1)
  val nsk2k1 = mkAdd(mkNSD(namespace, key2, canSignNamespaceDelegations = true), key1)
  val nsk2k3 = mkAdd(mkNSD(namespace, key2, canSignNamespaceDelegations = true), key3)
  val nsk2k1_remove = mkRemove(mkNSD(namespace, key2, canSignNamespaceDelegations = true), key1)
  val nsk2k1_nonRoot = mkAdd(mkNSD(namespace, key2, canSignNamespaceDelegations = false), key1)
  val nsk3k2 = mkAdd(mkNSD(namespace, key3, canSignNamespaceDelegations = true), key2)
  val nsk3k2_remove = mkRemove(mkNSD(namespace, key3, canSignNamespaceDelegations = true), key2)
  val nsk3k2_nonRoot = mkAdd(mkNSD(namespace, key3, canSignNamespaceDelegations = false), key2)
  val nsk3k1_nonRoot = mkAdd(mkNSD(namespace, key3, canSignNamespaceDelegations = false), key1)
  val nsk3k1_nonRoot_remove =
    mkRemove(mkNSD(namespace, key3, canSignNamespaceDelegations = false), key1)
  val nsk4k3 = mkAdd(mkNSD(namespace, key4, canSignNamespaceDelegations = true), key3)
  val nsk5k3_nonRoot = mkAdd(mkNSD(namespace, key5, canSignNamespaceDelegations = false), key3)

  val decentralizedNamespace =
    Namespace(Fingerprint.tryFromString("decentralized-namespace-fingerprint"))
  val ns1 = Namespace(key1.fingerprint)
  val ns2 = Namespace(key2.fingerprint)
  val ns3 = Namespace(key3.fingerprint)
  val owners = NonEmpty(Set, ns1, ns2, ns3)
  val decentralizedNamespaceDefinition =
    DecentralizedNamespaceDefinition
      .create(decentralizedNamespace, PositiveInt.two, owners)
      .fold(sys.error, identity)

  val ns1k1k1 = mkAdd(mkNSD(ns1, key1, canSignNamespaceDelegations = true), key1)
  val ns2k2k2 = mkAdd(mkNSD(ns2, key2, canSignNamespaceDelegations = true), key2)
  val ns2k2k2_remove = mkRemove(mkNSD(ns2, key2, canSignNamespaceDelegations = true), key2)
  val ns2k5k2 = mkAdd(mkNSD(ns2, key5, canSignNamespaceDelegations = true), key2)
  val ns2k5k2_remove = mkRemove(mkNSD(ns2, key5, canSignNamespaceDelegations = true), key2)
  val ns2k5k8 = mkAdd(mkNSD(ns2, key5, canSignNamespaceDelegations = true), key8)
  val ns2k2k5 = mkAdd(mkNSD(ns2, key2, canSignNamespaceDelegations = true), key5)
  val ns2k8k5 = mkAdd(mkNSD(ns2, key8, canSignNamespaceDelegations = true), key5)
  val ns2k8k5_remove = mkRemove(mkNSD(ns2, key8, canSignNamespaceDelegations = true), key5)
  val ns2k8k2_nonRoot = mkAdd(mkNSD(ns2, key8, canSignNamespaceDelegations = false), key2)
  val ns2k8k2_nonRoot_remove =
    mkRemove(mkNSD(ns2, key8, canSignNamespaceDelegations = false), key2)

  val ns3k3k3 = mkAdd(mkNSD(ns3, key3, canSignNamespaceDelegations = true), key3)

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

  /** @param canSignNamespaceDelegations
    *   if true, creates a namespace delegation that can sign all mappings. if false, creates a
    *   namespace delegation that cannot sign namespace delegations
    */
  def mkNSD(namespace: Namespace, key: SigningPublicKey, canSignNamespaceDelegations: Boolean) =
    NamespaceDelegation.tryCreate(
      namespace,
      key,
      if (canSignNamespaceDelegations) {
        // randomly choose between two ways a delegation can be permitted to sign namespace delegations.
        // this is to increase test coverage over time
        selectOneAtRandom(
          CanSignAllMappings,
          CanSignSpecificMappings(NonEmpty.from(TopologyMapping.Code.all.toSet).value),
        )
      } else
        // randomly choose between two ways a delegation can be prohibite to sign namespace delegations.
        // this is to increase test coverage over time
        selectOneAtRandom(
          CanSignAllButNamespaceDelegations,
          CanSignSpecificMappings(
            NonEmpty.from(TopologyMapping.Code.all.toSet - NamespaceDelegation.code).value
          ),
        ),
    )

  private def selectOneAtRandom(
      a: DelegationRestriction,
      b: DelegationRestriction,
  ): DelegationRestriction =
    if (random.nextBoolean()) a else b

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
    authTx.copy(transaction =
      authTx.transaction.copy(signatures =
        NonEmpty.mk(Set, SingleTransactionSignature(authTx.transaction.hash, signature))
      )
    )
  }

  def check(
      graph: AuthorizationCheck,
      mappingToAuthorize: TopologyMapping.Code,
      valid: Boolean,
  )(keys: SigningPublicKey*): Assertion =
    graph.existsAuthorizedKeyIn(
      keys.map(_.fingerprint).toSet,
      mappingToAuthorize,
    ) shouldBe valid

}
