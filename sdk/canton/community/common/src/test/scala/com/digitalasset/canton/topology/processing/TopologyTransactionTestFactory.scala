// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.DefaultTestIdentities.sequencerId
import com.digitalasset.canton.topology.transaction.*

import scala.concurrent.ExecutionContext

class TopologyTransactionTestFactory(loggerFactory: NamedLoggerFactory, initEc: ExecutionContext)
    extends TestingOwnerWithKeys(sequencerId, loggerFactory, initEc) {

  import SigningKeys.*

  def createNs(ns: Namespace, key: SigningPublicKey, isRootDelegation: Boolean) =
    NamespaceDelegation.tryCreate(ns, key, isRootDelegation)

  val ns1 = Namespace(key1.fingerprint)
  val ns1_unsupportedSpec = Namespace(key1_unsupportedSpec.fingerprint)
  val ns2 = Namespace(key2.fingerprint)
  val ns3 = Namespace(key3.fingerprint)
  val ns4 = Namespace(key4.fingerprint)
  val ns6 = Namespace(key6.fingerprint)
  val ns7 = Namespace(key7.fingerprint)
  val ns8 = Namespace(key8.fingerprint)
  val ns9 = Namespace(key9.fingerprint)
  val uid1a = UniqueIdentifier.tryCreate("one", ns1)
  val uid1b = UniqueIdentifier.tryCreate("two", ns1)
  val uid6 = UniqueIdentifier.tryCreate("other", ns6)
  val synchronizerId1 = SynchronizerId(UniqueIdentifier.tryCreate("domain", ns1))
  val synchronizerId1a = SynchronizerId(uid1a)
  val party1b = PartyId(uid1b)
  val party6 = PartyId(uid6)
  val participant1 = ParticipantId(uid1a)
  val participant6 = ParticipantId(uid6)
  val ns1k1_k1 = mkAdd(createNs(ns1, key1, isRootDelegation = true), key1)
  val ns1k1_k1_unsupportedScheme =
    mkAdd(
      createNs(ns1_unsupportedSpec, key1_unsupportedSpec, isRootDelegation = true),
      key1_unsupportedSpec,
    )
  val ns1k2_k1 = mkAdd(createNs(ns1, key2, isRootDelegation = true), key1)
  val ns1k2_k1p = mkAdd(createNs(ns1, key2, isRootDelegation = true), key1)
  val ns1k3_k2 = mkAdd(createNs(ns1, key3, isRootDelegation = false), key2)
  val ns1k8_k3_fail = mkAdd(createNs(ns1, key8, isRootDelegation = false), key3)
  val ns2k2_k2 = mkAdd(createNs(ns2, key2, isRootDelegation = true), key2)
  val ns3k3_k3 = mkAdd(createNs(ns3, key3, isRootDelegation = true), key3)
  val ns6k3_k6 = mkAdd(createNs(ns6, key3, isRootDelegation = false), key6)
  val ns6k6_k6 = mkAdd(createNs(ns6, key6, isRootDelegation = true), key6)
  val id1ak4_k1 = mkAdd(IdentifierDelegation(uid1a, key4), key1)
  val id1ak4_k2 = mkAdd(IdentifierDelegation(uid1a, key4), key2)
  val id1ak6_k4 = mkAdd(IdentifierDelegation(uid1a, key6), key4)

  val id6k4_k1 = mkAdd(IdentifierDelegation(uid6, key4), key1)

  val okm1ak5k1E_k2 =
    mkAddMultiKey(
      OwnerToKeyMapping(participant1, NonEmpty(Seq, key5, EncryptionKeys.key1)),
      NonEmpty(Set, key2, key5),
    )
  val okm1bk5k1E_k1 =
    mkAddMultiKey(
      OwnerToKeyMapping(participant1, NonEmpty(Seq, key5, EncryptionKeys.key1)),
      NonEmpty(Set, key1, key5),
    )
  val okm1bk5k1E_k4 =
    mkAddMultiKey(
      OwnerToKeyMapping(participant1, NonEmpty(Seq, key5, EncryptionKeys.key1)),
      NonEmpty(Set, key4, key5),
    )

  val sequencer1 = SequencerId(UniqueIdentifier.tryCreate("sequencer1", ns1))
  val okmS1k7_k1 =
    mkAddMultiKey(
      OwnerToKeyMapping(sequencer1, NonEmpty(Seq, key7)),
      NonEmpty(Set, key1, key7),
    )
  val sdmS1_k1 =
    mkAdd(
      SequencerSynchronizerState
        .create(synchronizerId1, PositiveInt.one, Seq(sequencer1), Seq.empty)
        .getOrElse(sys.error("Failed to create SequencerDomainState")),
      key1,
    )

  val dtcp1_k1 =
    mkAdd(SynchronizerTrustCertificate(participant1, SynchronizerId(uid1a)), key1)

  val defaultSynchronizerParameters = TestSynchronizerParameters.defaultDynamic

  val p1p1B_k2 =
    mkAdd(
      PartyToParticipant.tryCreate(
        party1b,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant1, ParticipantPermission.Submission)),
      ),
      key2,
    )
  val p1p6_k2 =
    mkAdd(
      PartyToParticipant.tryCreate(
        party1b,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermission.Submission)),
      ),
      key2,
      isProposal = true,
    )
  val p1p6_k6 =
    mkAddMultiKey(
      PartyToParticipant.tryCreate(
        party1b,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermission.Submission)),
      ),
      NonEmpty(Set, key1, key6),
      isProposal = true,
    )
  val p1p6_k2k6 =
    mkAddMultiKey(
      PartyToParticipant.tryCreate(
        party1b,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermission.Submission)),
      ),
      NonEmpty(Set, key2, key6),
    )

  val p1p6B_k3 =
    mkAdd(
      PartyToParticipant.tryCreate(
        party1b,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermission.Submission)),
      ),
      key3,
    )

  val dmp1_k2 = mkAdd(
    SynchronizerParametersState(SynchronizerId(uid1a), defaultSynchronizerParameters),
    key2,
  )

  val dmp1_k1 = mkAdd(
    SynchronizerParametersState(
      SynchronizerId(uid1a),
      defaultSynchronizerParameters
        .tryUpdate(confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(1)),
    ),
    key1,
  )

  val dmp1_k1_bis = mkAdd(
    SynchronizerParametersState(
      SynchronizerId(uid1a),
      defaultSynchronizerParameters
        .tryUpdate(confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(2)),
    ),
    key1,
  )

  val ns7k7_k7 = mkAdd(createNs(ns7, key7, isRootDelegation = true), key7)
  val ns8k8_k8 = mkAdd(createNs(ns8, key8, isRootDelegation = true), key8)
  val ns9k9_k9 = mkAdd(createNs(ns9, key9, isRootDelegation = true), key9)

  val dns1 = mkAddMultiKey(
    DecentralizedNamespaceDefinition
      .create(ns7, PositiveInt.two, NonEmpty(Set, ns1, ns8, ns9))
      .fold(
        err => sys.error(s"Failed to create DecentralizedNamespaceDefinition 1: $err"),
        identity,
      ),
    NonEmpty(Set, key1, key8, key9),
    serial = PositiveInt.one,
  )
  val dns1Removal = mkRemove(
    dns1.mapping,
    NonEmpty(Set, key1, key8, key9),
    serial = PositiveInt.two,
  )
  val dns1Idd = mkAddMultiKey(
    IdentifierDelegation(UniqueIdentifier.tryCreate("test", dns1.mapping.namespace), key4),
    NonEmpty(Set, key1, key8, key9),
  )
  val dns2 = mkAdd(
    DecentralizedNamespaceDefinition
      .create(ns7, PositiveInt.one, NonEmpty(Set, ns1))
      .fold(
        err => sys.error(s"Failed to create DecentralizedNamespaceDefinition 2: $err"),
        identity,
      ),
    key9,
    serial = PositiveInt.two,
    isProposal = true,
  )
  val dns3 = mkAdd(
    DecentralizedNamespaceDefinition
      .create(ns7, PositiveInt.one, NonEmpty(Set, ns1))
      .fold(
        err => sys.error(s"Failed to create DecentralizedNamespaceDefinition 3: $err"),
        identity,
      ),
    key8,
    serial = PositiveInt.two,
    isProposal = true,
  )
  val decentralizedNamespaceOwners = List(ns1k1_k1, ns8k8_k8, ns9k9_k9)
  val decentralizedNamespaceWithMultipleOwnerThreshold =
    List(ns1k1_k1, ns8k8_k8, ns9k9_k9, dns1)

  private val dndOwners =
    NonEmpty(Set, key1.fingerprint, key2.fingerprint, key3.fingerprint).map(Namespace(_))
  private val dndNamespace = DecentralizedNamespaceDefinition.computeNamespace(dndOwners)
  val dnd_proposal_k1 = mkAdd(
    DecentralizedNamespaceDefinition
      .create(
        dndNamespace,
        PositiveInt.two,
        dndOwners,
      )
      .fold(sys.error, identity),
    signingKey = key1,
    isProposal = true,
  )
  val dnd_proposal_k2 = mkAdd(
    DecentralizedNamespaceDefinition
      .create(
        dndNamespace,
        PositiveInt.two,
        NonEmpty(Set, key1.fingerprint, key2.fingerprint, key3.fingerprint).map(Namespace(_)),
      )
      .fold(sys.error, identity),
    signingKey = key2,
    isProposal = true,
  )
  // this only differs from dnd_proposal_k2 by having a different threshold
  val dnd_proposal_k2_alternative = mkAdd(
    DecentralizedNamespaceDefinition
      .create(
        dndNamespace,
        PositiveInt.one,
        NonEmpty(Set, key1.fingerprint, key2.fingerprint, key3.fingerprint).map(Namespace(_)),
      )
      .fold(sys.error, identity),
    signingKey = key2,
    isProposal = true,
  )

  val dnd_proposal_k3 = mkAdd(
    DecentralizedNamespaceDefinition
      .create(
        dndNamespace,
        PositiveInt.two,
        NonEmpty(Set, key1.fingerprint, key2.fingerprint, key3.fingerprint).map(Namespace(_)),
      )
      .fold(sys.error, identity),
    signingKey = key3,
    isProposal = true,
  )
}
