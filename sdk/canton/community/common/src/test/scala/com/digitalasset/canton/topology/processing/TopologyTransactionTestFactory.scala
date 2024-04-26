// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, SigningPublicKey}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DefaultTestIdentities.sequencerIdX
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*

import scala.concurrent.ExecutionContext

class TopologyTransactionTestFactory(loggerFactory: NamedLoggerFactory, initEc: ExecutionContext)
    extends TestingOwnerWithKeys(sequencerIdX, loggerFactory, initEc) {

  import SigningKeys.*

  def createNsX(ns: Namespace, key: SigningPublicKey, isRootDelegation: Boolean) =
    NamespaceDelegation.tryCreate(ns, key, isRootDelegation)

  val ns1 = Namespace(key1.fingerprint)
  val ns2 = Namespace(key2.fingerprint)
  val ns3 = Namespace(key3.fingerprint)
  val ns4 = Namespace(key4.fingerprint)
  val ns6 = Namespace(key6.fingerprint)
  val ns7 = Namespace(key7.fingerprint)
  val ns8 = Namespace(key8.fingerprint)
  val ns9 = Namespace(key9.fingerprint)
  val domainId1 = DomainId(UniqueIdentifier(Identifier.tryCreate("domain"), ns1))
  val uid1a = UniqueIdentifier(Identifier.tryCreate("one"), ns1)
  val uid1b = UniqueIdentifier(Identifier.tryCreate("two"), ns1)
  val uid6 = UniqueIdentifier(Identifier.tryCreate("other"), ns6)
  val party1b = PartyId(uid1b)
  val party6 = PartyId(uid6)
  val participant1 = ParticipantId(uid1a)
  val participant6 = ParticipantId(uid6)
  val ns1k1_k1 = mkAdd(createNsX(ns1, key1, isRootDelegation = true), key1)
  val ns1k2_k1 = mkAdd(createNsX(ns1, key2, isRootDelegation = true), key1)
  val ns1k2_k1p = mkAdd(createNsX(ns1, key2, isRootDelegation = true), key1)
  val ns1k3_k2 = mkAdd(createNsX(ns1, key3, isRootDelegation = false), key2)
  val ns1k8_k3_fail = mkAdd(createNsX(ns1, key8, isRootDelegation = false), key3)
  val ns2k2_k2 = mkAdd(createNsX(ns2, key2, isRootDelegation = true), key2)
  val ns3k3_k3 = mkAdd(createNsX(ns3, key3, isRootDelegation = true), key3)
  val ns6k3_k6 = mkAdd(createNsX(ns6, key3, isRootDelegation = false), key6)
  val ns6k6_k6 = mkAdd(createNsX(ns6, key6, isRootDelegation = true), key6)
  val id1ak4_k2 = mkAdd(IdentifierDelegation(uid1a, key4), key2)
  val id1ak4_k2p = mkAdd(IdentifierDelegation(uid1a, key4), key2)
  val id1ak4_k1 = mkAdd(IdentifierDelegation(uid1a, key4), key1)

  val id6k4_k1 = mkAdd(IdentifierDelegation(uid6, key4), key1)

  val okm1ak5_k3 =
    mkAdd(OwnerToKeyMapping(participant1, Some(domainId1), NonEmpty(Seq, key5)), key3)
  val okm1ak1E_k3 = mkAdd(
    OwnerToKeyMapping(participant1, Some(domainId1), NonEmpty(Seq, EncryptionKeys.key1)),
    key3,
  )
  val okm1ak5_k2 =
    mkAdd(OwnerToKeyMapping(participant1, Some(domainId1), NonEmpty(Seq, key5)), key2)
  val okm1bk5_k1 =
    mkAdd(OwnerToKeyMapping(participant1, Some(domainId1), NonEmpty(Seq, key5)), key1)
  val okm1bk5_k4 =
    mkAdd(OwnerToKeyMapping(participant1, Some(domainId1), NonEmpty(Seq, key5)), key4)

  val sequencer1 = SequencerId(UniqueIdentifier(Identifier.tryCreate("sequencer1"), ns1))
  val okmS1k7_k1 =
    mkAdd(OwnerToKeyMapping(sequencer1, Some(domainId1), NonEmpty(Seq, key7)), key1)
  val sdmS1_k1 =
    mkAdd(
      SequencerDomainState
        .create(domainId1, PositiveInt.one, Seq(sequencer1), Seq.empty)
        .getOrElse(sys.error("Failed to create SequencerDomainStateX")),
      key1,
    )
  def add_OkmS1k9_k1(otk: OwnerToKeyMapping, serial: PositiveInt) =
    mkAdd(otk.copy(keys = otk.keys :+ key9), key1)
  def remove_okmS1k7_k1(otk: OwnerToKeyMapping, serial: PositiveInt) = {
    NonEmpty
      .from(otk.keys.forgetNE.toSet - key7)
      .map(keys => mkAdd(otk.copy(keys = keys.toSeq)))
      .getOrElse(sys.error(s"tried to remove the last key of $otk"))
  }

  val dtcp1_k1 = mkAdd(DomainTrustCertificate(participant1, domainId1, false, Seq.empty), key1)

  val defaultDomainParameters = TestDomainParameters.defaultDynamic

  val p1p1B_k2 =
    mkAdd(
      PartyToParticipant(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant1, ParticipantPermission.Submission)),
        groupAddressing = false,
      ),
      key2,
    )
  val p1p6_k2 =
    mkAdd(
      PartyToParticipant(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermission.Submission)),
        groupAddressing = false,
      ),
      key2,
      isProposal = true,
    )
  val p1p6_k6 =
    mkAdd(
      PartyToParticipant(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermission.Submission)),
        groupAddressing = false,
      ),
      key6,
      isProposal = true,
    )
  val p1p6_k2k6 =
    mkAddMultiKey(
      PartyToParticipant(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermission.Submission)),
        groupAddressing = false,
      ),
      NonEmpty(Set, key2, key6),
    )

  val p1p6B_k3 =
    mkAdd(
      PartyToParticipant(
        party1b,
        Some(domainId1),
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermission.Submission)),
        groupAddressing = false,
      ),
      key3,
    )

  val dmp1_k2 = mkAdd(
    DomainParametersState(DomainId(uid1a), defaultDomainParameters),
    key2,
  )

  val dmp1_k1 = mkAdd(
    DomainParametersState(
      DomainId(uid1a),
      defaultDomainParameters
        .tryUpdate(confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(1)),
    ),
    key1,
  )

  val dmp1_k1_bis = mkAdd(
    DomainParametersState(
      DomainId(uid1a),
      defaultDomainParameters
        .tryUpdate(confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(2)),
    ),
    key1,
  )

  val ns7k7_k7 = mkAdd(createNsX(ns7, key7, isRootDelegation = true), key7)
  val ns8k8_k8 = mkAdd(createNsX(ns8, key8, isRootDelegation = true), key8)
  val ns9k9_k9 = mkAdd(createNsX(ns9, key9, isRootDelegation = true), key9)

  val dns1 = mkAddMultiKey(
    DecentralizedNamespaceDefinition
      .create(ns7, PositiveInt.two, NonEmpty(Set, ns1, ns8, ns9))
      .fold(
        err => sys.error(s"Failed to create DecentralizedNamespaceDefinitionX 1: $err"),
        identity,
      ),
    NonEmpty(Set, key1, key8, key9),
    serial = PositiveInt.one,
  )
  val dns2 = mkAdd(
    DecentralizedNamespaceDefinition
      .create(ns7, PositiveInt.one, NonEmpty(Set, ns1))
      .fold(
        err => sys.error(s"Failed to create DecentralizedNamespaceDefinitionX 2: $err"),
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
        err => sys.error(s"Failed to create DecentralizedNamespaceDefinitionX 3: $err"),
        identity,
      ),
    key8,
    serial = PositiveInt.two,
    isProposal = true,
  )
  val decentralizedNamespaceWithMultipleOwnerThreshold =
    List(ns1k1_k1, ns8k8_k8, ns9k9_k9, ns7k7_k7, dns1)

  val dnd_proposal_k1 = mkAdd(
    DecentralizedNamespaceDefinition
      .create(
        Namespace(Fingerprint.tryCreate("dnd-namespace")),
        PositiveInt.two,
        NonEmpty(Set, key1.fingerprint, key2.fingerprint, key3.fingerprint).map(Namespace(_)),
      )
      .fold(sys.error, identity),
    signingKey = key1,
    isProposal = true,
  )
  val dnd_proposal_k2 = mkAdd(
    DecentralizedNamespaceDefinition
      .create(
        Namespace(Fingerprint.tryCreate("dnd-namespace")),
        PositiveInt.two,
        NonEmpty(Set, key1.fingerprint, key2.fingerprint, key3.fingerprint).map(Namespace(_)),
      )
      .fold(sys.error, identity),
    signingKey = key2,
    isProposal = true,
  )
  val dnd_proposal_k3 = mkAdd(
    DecentralizedNamespaceDefinition
      .create(
        Namespace(Fingerprint.tryCreate("dnd-namespace")),
        PositiveInt.two,
        NonEmpty(Set, key1.fingerprint, key2.fingerprint, key3.fingerprint).map(Namespace(_)),
      )
      .fold(sys.error, identity),
    signingKey = key3,
    isProposal = true,
  )
}
