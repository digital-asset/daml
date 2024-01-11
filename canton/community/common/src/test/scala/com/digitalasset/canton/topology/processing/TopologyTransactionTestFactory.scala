// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DefaultTestIdentities.domainManager
import com.digitalasset.canton.topology.transaction.{
  DomainParametersChange,
  IdentifierDelegation,
  NamespaceDelegation,
  OwnerToKeyMapping,
  ParticipantPermission,
  ParticipantState,
  PartyToParticipant,
  RequestSide,
  TrustLevel,
}
import com.digitalasset.canton.topology.{
  DomainId,
  Identifier,
  Namespace,
  ParticipantId,
  PartyId,
  SequencerId,
  TestingOwnerWithKeys,
  UniqueIdentifier,
}

import scala.concurrent.ExecutionContext

class TopologyTransactionTestFactory(loggerFactory: NamedLoggerFactory, initEc: ExecutionContext)
    extends TestingOwnerWithKeys(domainManager, loggerFactory, initEc) {

  import SigningKeys.*

  val ns1 = Namespace(key1.fingerprint)
  val ns6 = Namespace(key6.fingerprint)
  val domainId = DomainId(UniqueIdentifier(Identifier.tryCreate("domain"), ns1))
  val uid1a = UniqueIdentifier(Identifier.tryCreate("one"), ns1)
  val uid1b = UniqueIdentifier(Identifier.tryCreate("two"), ns1)
  val uid6 = UniqueIdentifier(Identifier.tryCreate("other"), ns6)
  val party1b = PartyId(uid1b)
  val party2 = PartyId(uid6)
  val participant1 = ParticipantId(uid1a)
  val participant6 = ParticipantId(uid6)
  val ns1k1_k1 = mkAdd(NamespaceDelegation(ns1, key1, isRootDelegation = true), key1)
  val ns1k2_k1 = mkAdd(NamespaceDelegation(ns1, key2, isRootDelegation = true), key1)
  val ns1k2_k1p = mkAdd(NamespaceDelegation(ns1, key2, isRootDelegation = true), key1)
  val ns1k3_k2 = mkAdd(NamespaceDelegation(ns1, key3, isRootDelegation = false), key2)
  val ns1k8_k3_fail = mkAdd(NamespaceDelegation(ns1, key8, isRootDelegation = false), key3)
  val ns6k3_k6 = mkAdd(NamespaceDelegation(ns6, key3, isRootDelegation = false), key6)
  val ns6k6_k6 = mkAdd(NamespaceDelegation(ns6, key6, isRootDelegation = true), key6)
  val id1ak4_k2 = mkAdd(IdentifierDelegation(uid1a, key4), key2)
  val id1ak4_k2p = mkAdd(IdentifierDelegation(uid1a, key4), key2)
  val id1ak4_k1 = mkAdd(IdentifierDelegation(uid1a, key4), key1)

  val id6k4_k1 = mkAdd(IdentifierDelegation(uid6, key4), key1)

  val okm1ak5_k3 = mkAdd(OwnerToKeyMapping(participant1, key5), key3)
  val okm1ak1E_k3 = mkAdd(OwnerToKeyMapping(participant1, EncryptionKeys.key1), key3)
  val okm1ak5_k2 = mkAdd(OwnerToKeyMapping(participant1, key5), key2)
  val okm1bk5_k1 = mkAdd(OwnerToKeyMapping(participant1, key5), key1)
  val okm1bk5_k4 = mkAdd(OwnerToKeyMapping(participant1, key5), key4)

  val sequencer1 = SequencerId(UniqueIdentifier(Identifier.tryCreate("sequencer1"), ns1))
  val okmS1k7_k1 = mkAdd(OwnerToKeyMapping(sequencer1, key7), key1)
  val okmS1k9_k1 = mkAdd(OwnerToKeyMapping(sequencer1, key9), key1)
  val okmS1k7_k1_remove = mkTrans(okmS1k7_k1.transaction.reverse)

  val ps1d1T_k3 = mkAdd(
    ParticipantState(
      RequestSide.To,
      domainManager.domainId,
      participant1,
      ParticipantPermission.Submission,
      TrustLevel.Ordinary,
    ),
    key3,
  )
  val ps1d1F_k1 = mkAdd(
    ParticipantState(
      RequestSide.From,
      domainManager.domainId,
      participant1,
      ParticipantPermission.Submission,
      TrustLevel.Ordinary,
    ),
    key1,
  )

  val defaultDomainParameters = TestDomainParameters.defaultDynamic

  val p1p1B_k2 =
    mkAdd(
      PartyToParticipant(RequestSide.Both, party1b, participant1, ParticipantPermission.Submission),
      key2,
    )
  val p1p2F_k2 =
    mkAdd(
      PartyToParticipant(RequestSide.From, party1b, participant6, ParticipantPermission.Submission),
      key2,
    )
  val p1p2T_k6 =
    mkAdd(
      PartyToParticipant(RequestSide.To, party1b, participant6, ParticipantPermission.Submission),
      key6,
    )
  val p1p2B_k3 =
    mkAdd(
      PartyToParticipant(RequestSide.Both, party1b, participant6, ParticipantPermission.Submission),
      key3,
    )

  val dmp1_k2 = mkDmGov(
    DomainParametersChange(DomainId(uid1a), defaultDomainParameters),
    key2,
  )

  val dmp1_k1 = mkDmGov(
    DomainParametersChange(
      DomainId(uid1a),
      defaultDomainParameters
        .tryUpdate(participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(1)),
    ),
    key1,
  )

  val dmp1_k1_bis = mkDmGov(
    DomainParametersChange(
      DomainId(uid1a),
      defaultDomainParameters
        .tryUpdate(participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(2)),
    ),
    key1,
  )

}
