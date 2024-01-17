// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DefaultTestIdentities.domainManager
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*

import scala.concurrent.ExecutionContext

class TopologyTransactionTestFactoryX(loggerFactory: NamedLoggerFactory, initEc: ExecutionContext)
    extends TestingOwnerWithKeysX(domainManager, loggerFactory, initEc) {

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
  val ns1k1_k1 = mkAdd(NamespaceDelegationX.tryCreate(ns1, key1, isRootDelegation = true), key1)
  val ns1k2_k1 = mkAdd(NamespaceDelegationX.tryCreate(ns1, key2, isRootDelegation = true), key1)
  val ns1k2_k1p = mkAdd(NamespaceDelegationX.tryCreate(ns1, key2, isRootDelegation = true), key1)
  val ns1k3_k2 = mkAdd(NamespaceDelegationX.tryCreate(ns1, key3, isRootDelegation = false), key2)
  val ns1k8_k3_fail =
    mkAdd(NamespaceDelegationX.tryCreate(ns1, key8, isRootDelegation = false), key3)
  val ns6k3_k6 = mkAdd(NamespaceDelegationX.tryCreate(ns6, key3, isRootDelegation = false), key6)
  val ns6k6_k6 = mkAdd(NamespaceDelegationX.tryCreate(ns6, key6, isRootDelegation = true), key6)
  val id1ak4_k2 = mkAdd(IdentifierDelegationX(uid1a, key4), key2)
  val id1ak4_k2p = mkAdd(IdentifierDelegationX(uid1a, key4), key2)
  val id1ak4_k1 = mkAdd(IdentifierDelegationX(uid1a, key4), key1)

  val id6k4_k1 = mkAdd(IdentifierDelegationX(uid6, key4), key1)

  val okm1ak5_k3 = mkAdd(OwnerToKeyMappingX(participant1, None, NonEmpty(Seq, key5)), key3)
  val okm1ak1E_k3 =
    mkAdd(OwnerToKeyMappingX(participant1, None, NonEmpty(Seq, EncryptionKeys.key1)), key3)
  val okm1ak5_k2 = mkAdd(OwnerToKeyMappingX(participant1, None, NonEmpty(Seq, key5)), key2)
  val okm1bk5_k1 = mkAdd(OwnerToKeyMappingX(participant1, None, NonEmpty(Seq, key5)), key1)
  val okm1bk5_k4 = mkAdd(OwnerToKeyMappingX(participant1, None, NonEmpty(Seq, key5)), key4)

  val sequencer1 = SequencerId(UniqueIdentifier(Identifier.tryCreate("sequencer1"), ns1))
  val okmS1k7_k1 = mkAdd(OwnerToKeyMappingX(sequencer1, None, NonEmpty(Seq, key7)), key1)
  def add_OkmS1k9_k1(otk: OwnerToKeyMappingX, serial: PositiveInt) =
    mkAdd(otk.copy(keys = otk.keys :+ key9), key1)
  def remove_okmS1k7_k1(otk: OwnerToKeyMappingX, serial: PositiveInt) = {
    NonEmpty
      .from(otk.keys.forgetNE.toSet - key7)
      .map(keys => mkAdd(otk.copy(keys = keys.toSeq)))
      .getOrElse(sys.error(s"tried to remove the last key of $otk"))
  }

  val dtcp1_k1 = mkAdd(DomainTrustCertificateX(participant1, domainId, false, Seq.empty), key1)

  val defaultDomainParameters = TestDomainParameters.defaultDynamic

  val p1p1B_k2 =
    mkAdd(
      PartyToParticipantX(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant1, ParticipantPermissionX.Submission)),
        groupAddressing = false,
      ),
      key2,
    )
  val p1p2F_k2 =
    mkAdd(
      PartyToParticipantX(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermissionX.Submission)),
        groupAddressing = false,
      ),
      key2,
    )
  val p1p2T_k6 =
    mkAdd(
      PartyToParticipantX(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermissionX.Submission)),
        groupAddressing = false,
      ),
      key6,
    )
  val p1p2B_k3 =
    mkAdd(
      PartyToParticipantX(
        party1b,
        None,
        threshold = PositiveInt.one,
        Seq(HostingParticipant(participant6, ParticipantPermissionX.Submission)),
        groupAddressing = false,
      ),
      key3,
    )

  val dmp1_k2 = mkAdd(
    DomainParametersStateX(DomainId(uid1a), defaultDomainParameters),
    key2,
  )

  val dmp1_k1 = mkAdd(
    DomainParametersStateX(
      DomainId(uid1a),
      defaultDomainParameters
        .tryUpdate(participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(1)),
    ),
    key1,
  )

  val dmp1_k1_bis = mkAdd(
    DomainParametersStateX(
      DomainId(uid1a),
      defaultDomainParameters
        .tryUpdate(participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(2)),
    ),
    key1,
  )

}
