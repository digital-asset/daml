// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertions.fail
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

@nowarn("msg=match may not be exhaustive")
class TopologyStoreTestData(
    testedProtocolVersion: ProtocolVersion,
    loggerFactory: NamedLoggerFactory,
    executionContext: ExecutionContext,
) {

  def makeSignedTx[Op <: TopologyChangeOp, M <: TopologyMapping](
      mapping: M,
      op: Op = TopologyChangeOp.Replace,
      isProposal: Boolean = false,
      serial: PositiveInt = PositiveInt.one,
  )(signingKeys: SigningPublicKey*): SignedTopologyTransaction[Op, M] = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*
    val tx = TopologyTransaction(
      op,
      serial,
      mapping,
      ProtocolVersion.v33,
    )
    val signatures = NonEmpty
      .from(
        signingKeys.toSeq.map(key =>
          factory.cryptoApi.crypto.privateCrypto
            .sign(tx.hash.hash, key.fingerprint)
            .value
            .onShutdown(fail("shutdown"))(
              DirectExecutionContext(loggerFactory.getLogger(this.getClass))
            )
            .futureValue
            .getOrElse(fail(s"error"))
        )
      )
      .getOrElse(fail("no keys provided"))
      .toSet

    SignedTopologyTransaction.apply[Op, M](
      tx,
      signatures = signatures,
      isProposal = isProposal,
    )(
      SignedTopologyTransaction.supportedProtoVersions
        .protocolVersionRepresentativeFor(
          ProtocolVersion.v33
        )
    )
  }

  val Seq(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10) =
    (1L to 10L).map(CantonTimestamp.Epoch.plusSeconds)

  val factory: TestingOwnerWithKeys =
    new TestingOwnerWithKeys(
      SequencerId(
        UniqueIdentifier.tryCreate("da", "sequencer")
      ),
      loggerFactory,
      executionContext,
    )

  val p1Key = factory.SigningKeys.key1
  val p1Namespace = Namespace(p1Key.fingerprint)
  val p1Id = ParticipantId(UniqueIdentifier.tryCreate("participant1", p1Namespace))
  val party1 = PartyId.tryCreate("party1", p1Namespace)
  val party2 = PartyId.tryCreate("party2", p1Namespace)
  val party3 = PartyId.tryCreate("party3", p1Namespace)

  val p2Key = factory.SigningKeys.key2
  val p2Namespace = Namespace(p2Key.fingerprint)
  val p2Id = ParticipantId(UniqueIdentifier.tryCreate("participant2", p2Namespace))

  val p3Key = factory.SigningKeys.key3
  val p3Namespace = Namespace(p3Key.fingerprint)
  val p3Id = ParticipantId(UniqueIdentifier.tryCreate("participant3", p3Namespace))

  val dnd_p1p2_keys = NonEmpty(Seq, p1Key, p2Key)
  val dns_p1p2 = DecentralizedNamespaceDefinition.computeNamespace(
    Set(p1Namespace, p2Namespace)
  )
  val da_p1p2_domainId = DomainId(
    UniqueIdentifier.tryCreate(
      "da",
      dns_p1p2,
    )
  )

  val medKey = factory.SigningKeys.key4
  val medNamespace = Namespace(medKey.fingerprint)

  val seqKey = factory.SigningKeys.key5
  val seqNamespace = Namespace(seqKey.fingerprint)

  val dns_p1seq = DecentralizedNamespaceDefinition.computeNamespace(
    Set(p1Namespace, seqNamespace)
  )

  val domain1_p1p2_domainId = DomainId(
    UniqueIdentifier.tryCreate("domain1", dns_p1p2)
  )
  val med1Id = MediatorId(UniqueIdentifier.tryCreate("mediator1", medNamespace))
  val med2Id = MediatorId(UniqueIdentifier.tryCreate("mediator2", medNamespace))
  val seq1Id = SequencerId(UniqueIdentifier.tryCreate("sequencer1", seqNamespace))
  val seq2Id = SequencerId(UniqueIdentifier.tryCreate("sequencer2", seqNamespace))

  val `fred::p2Namepsace` = PartyId(UniqueIdentifier.tryCreate("fred", p2Namespace))

  val nsd_p1 = makeSignedTx(
    NamespaceDelegation
      .tryCreate(p1Namespace, p1Key, isRootDelegation = true)
  )(p1Key)

  val nsd_p2 = makeSignedTx(
    NamespaceDelegation
      .tryCreate(p2Namespace, p2Key, isRootDelegation = true)
  )(p2Key)

  val dnd_p1p2 = makeSignedTx(
    DecentralizedNamespaceDefinition.tryCreate(
      dns_p1p2,
      threshold = PositiveInt.two,
      owners = dnd_p1p2_keys.map(k => Namespace(k.fingerprint)).toSet,
    )
  )(dnd_p1p2_keys*)
  val dop_domain1_proposal = makeSignedTx(
    DomainParametersState(
      domain1_p1p2_domainId,
      DynamicDomainParameters
        .initialValues(
          topologyChangeDelay = NonNegativeFiniteDuration.Zero,
          protocolVersion = testedProtocolVersion,
          mediatorReactionTimeout = NonNegativeFiniteDuration.Zero,
        ),
    ),
    isProposal = true,
  )(p1Key)

  val dop_domain1 = makeSignedTx(
    DomainParametersState(
      domain1_p1p2_domainId,
      DynamicDomainParameters
        .initialValues(
          topologyChangeDelay = NonNegativeFiniteDuration.Zero,
          protocolVersion = testedProtocolVersion,
        ),
    )
  )(dnd_p1p2_keys*)
  val otk_p1 = makeSignedTx(
    OwnerToKeyMapping(p1Id, NonEmpty(Seq, p1Key, factory.EncryptionKeys.key1))
  )((p1Key))
  val idd_daDomain_key1 = makeSignedTx(
    IdentifierDelegation(da_p1p2_domainId.uid, factory.SigningKeys.key1),
    serial = PositiveInt.tryCreate(1),
  )(dnd_p1p2_keys*)
  val idd_daDomain_key1_removal = makeSignedTx(
    IdentifierDelegation(da_p1p2_domainId.uid, factory.SigningKeys.key1),
    op = TopologyChangeOp.Remove,
    serial = PositiveInt.tryCreate(2),
  )(dnd_p1p2_keys*)
  val dtc_p1_domain1 = makeSignedTx(
    DomainTrustCertificate(
      p1Id,
      domain1_p1p2_domainId,
    )
  )(p1Key)

  val ptp_fred_p1_proposal = makeSignedTx(
    PartyToParticipant.tryCreate(
      partyId = `fred::p2Namepsace`,
      threshold = PositiveInt.one,
      participants = Seq(HostingParticipant(p1Id, ParticipantPermission.Submission)),
    ),
    isProposal = true,
  )(p1Key)
  val nsd_seq = makeSignedTx(
    NamespaceDelegation.tryCreate(seqNamespace, seqKey, isRootDelegation = true)
  )(seqKey)
  val nsd_seq_invalid = makeSignedTx(
    NamespaceDelegation.tryCreate(seqNamespace, seqKey, isRootDelegation = true)
  )(p1Key) // explicitly signing with the wrong key

  val dnd_p1seq = makeSignedTx(
    DecentralizedNamespaceDefinition
      .create(
        dns_p1seq,
        PositiveInt.one,
        owners = NonEmpty(Set, p1Namespace, seqNamespace),
      )
      .getOrElse(fail())
  )(p1Key, seqKey)
  val otk_p2_proposal = makeSignedTx(
    OwnerToKeyMapping(p2Id, NonEmpty(Seq, p1Key, factory.EncryptionKeys.key1)),
    isProposal = true,
    serial = PositiveInt.tryCreate(2),
  )(p1Key)
  val ptp_fred_p1 = makeSignedTx(
    PartyToParticipant.tryCreate(
      partyId = `fred::p2Namepsace`,
      threshold = PositiveInt.one,
      participants = Seq(HostingParticipant(p1Id, ParticipantPermission.Confirmation)),
    )
  )(p1Key, p2Key)
  val dtc_p2_domain1 = makeSignedTx(
    DomainTrustCertificate(
      p2Id,
      domain1_p1p2_domainId,
    )
  )(p2Key)
  val dtc_p2_domain1_update = makeSignedTx(
    DomainTrustCertificate(
      p2Id,
      domain1_p1p2_domainId,
    ),
    serial = PositiveInt.tryCreate(2),
  )(p2Key)
  val mds_med1_domain1 = makeSignedTx(
    MediatorDomainState
      .create(
        domain = domain1_p1p2_domainId,
        group = NonNegativeInt.one,
        threshold = PositiveInt.one,
        active = Seq(med1Id),
        observers = Seq.empty,
      )
      .getOrElse(fail())
  )(dnd_p1p2_keys*)

  val mds_med1_domain1_invalid = makeSignedTx(
    MediatorDomainState
      .create(
        domain = domain1_p1p2_domainId,
        group = NonNegativeInt.one,
        threshold = PositiveInt.one,
        active = Seq(med1Id),
        observers = Seq.empty,
      )
      .getOrElse(fail())
  )(seqKey)

  val mds_med1_domain1_update = makeSignedTx(
    MediatorDomainState
      .create(
        domain = domain1_p1p2_domainId,
        group = NonNegativeInt.one,
        threshold = PositiveInt.one,
        active = Seq(med1Id, med2Id),
        observers = Seq.empty,
      )
      .getOrElse(fail()),
    serial = PositiveInt.tryCreate(2),
  )(dnd_p1p2_keys*)

  val sds_seq1_domain1 = makeSignedTx(
    SequencerDomainState
      .create(
        domain = domain1_p1p2_domainId,
        threshold = PositiveInt.one,
        active = Seq(seq1Id),
        observers = Seq.empty,
      )
      .getOrElse(fail())
  )(dnd_p1p2_keys*)
}
