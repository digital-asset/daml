// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX.Broadcast
import com.digitalasset.canton.protocol.{v0, v1, v2, v3}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

final case class DomainTopologyTransactionMessage private (
    domainTopologyManagerSignature: Signature,
    transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    notSequencedAfter: Option[CantonTimestamp], // must be present for protocol version 5 and above
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      DomainTopologyTransactionMessage.type
    ]
) extends UnsignedProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with ProtocolMessageV2
    with UnsignedProtocolMessageV3 {

  require(
    representativeProtocolVersion.representative < ProtocolVersion.v5 || notSequencedAfter.nonEmpty,
    "Not sequenced after must be non-empty for protocol version v5 and above",
  )

  def hashToSign(hashOps: HashOps): Hash =
    DomainTopologyTransactionMessage.hash(
      transactions,
      domainId,
      notSequencedAfter,
      hashOps,
      representativeProtocolVersion.representative,
    )

  def toProtoV0: v0.DomainTopologyTransactionMessage = {
    v0.DomainTopologyTransactionMessage(
      signature = Some(domainTopologyManagerSignature.toProtoV0),
      transactions = transactions.map(_.getCryptographicEvidence),
      domainId = domainId.toProtoPrimitive,
    )
  }

  def toProtoV1: v1.DomainTopologyTransactionMessage = {
    v1.DomainTopologyTransactionMessage(
      signature = Some(domainTopologyManagerSignature.toProtoV0),
      transactions = transactions.map(_.getCryptographicEvidence),
      domainId = domainId.toProtoPrimitive,
      notSequencedAfter = notSequencedAfter.map(_.toProtoPrimitive),
    )
  }

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(
      v0.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV0)
    )

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(
      v1.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV0)
    )

  override def toProtoEnvelopeContentV2: v2.EnvelopeContent =
    v2.EnvelopeContent(
      v2.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV1)
    )

  override def toProtoSomeEnvelopeContentV3: v3.EnvelopeContent.SomeEnvelopeContent =
    v3.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV1)

  @transient override protected lazy val companionObj: DomainTopologyTransactionMessage.type =
    DomainTopologyTransactionMessage

  @VisibleForTesting
  def replaceSignatureForTesting(signature: Signature): DomainTopologyTransactionMessage = {
    copy(domainTopologyManagerSignature = signature)(representativeProtocolVersion)
  }

}

object DomainTopologyTransactionMessage
    extends HasProtocolVersionedCompanion[DomainTopologyTransactionMessage] {

  implicit val domainIdentityTransactionMessageCast
      : ProtocolMessageContentCast[DomainTopologyTransactionMessage] =
    ProtocolMessageContentCast.create[DomainTopologyTransactionMessage](
      "DomainTopologyTransactionMessage"
    ) {
      case ditm: DomainTopologyTransactionMessage => Some(ditm)
      case _ => None
    }

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(
      v0.DomainTopologyTransactionMessage
    )(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v5)(
      v1.DomainTopologyTransactionMessage
    )(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV0.toByteString,
    ),
  )

  private def hash(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      domainId: DomainId,
      notSequencedAfter: Option[CantonTimestamp],
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): Hash = {
    val builder = hashOps
      .build(HashPurpose.DomainTopologyTransactionMessageSignature)
      .add(domainId.toProtoPrimitive)
    if (protocolVersion >= ProtocolVersion.v5) {
      notSequencedAfter.foreach { ts =>
        builder.add(ts.toEpochMilli)
      }
    }
    transactions.foreach(elem => builder.add(elem.getCryptographicEvidence))
    builder.finish()
  }

  def create(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      syncCrypto: DomainSnapshotSyncCryptoApi,
      domainId: DomainId,
      notSequencedAfter: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SyncCryptoError, DomainTopologyTransactionMessage] = {
    val hashToSign =
      hash(
        transactions,
        domainId,
        Some(notSequencedAfter),
        syncCrypto.crypto.pureCrypto,
        protocolVersion,
      )
    syncCrypto
      .sign(hashToSign)
      .map(signature =>
        DomainTopologyTransactionMessage(
          signature,
          transactions,
          notSequencedAfter = Some(notSequencedAfter),
          domainId,
        )(protocolVersionRepresentativeFor(protocolVersion))
      )
  }

  def tryCreate(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      crypto: DomainSnapshotSyncCryptoApi,
      domainId: DomainId,
      notSequencedAfter: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[DomainTopologyTransactionMessage] =
    create(transactions, crypto, domainId, notSequencedAfter, protocolVersion).fold(
      err =>
        throw new IllegalStateException(
          s"Failed to create domain topology transaction message: $err"
        ),
      identity,
    )

  def fromProtoV0(
      message: v0.DomainTopologyTransactionMessage
  ): ParsingResult[DomainTopologyTransactionMessage] = {
    val v0.DomainTopologyTransactionMessage(signature, domainId, transactions) = message
    for {
      succeededContent <- transactions.toList.traverse(SignedTopologyTransaction.fromByteString)
      signature <- ProtoConverter.parseRequired(Signature.fromProtoV0, "signature", signature)
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
    } yield DomainTopologyTransactionMessage(
      signature,
      succeededContent,
      notSequencedAfter = None,
      DomainId(domainUid),
    )(protocolVersionRepresentativeFor(ProtoVersion(0)))
  }

  def fromProtoV1(
      message: v1.DomainTopologyTransactionMessage
  ): ParsingResult[DomainTopologyTransactionMessage] = {
    val v1.DomainTopologyTransactionMessage(signature, domainId, timestamp, transactions) = message
    for {
      succeededContent <- transactions.toList.traverse(
        SignedTopologyTransaction.fromByteString
      )
      signature <- ProtoConverter.parseRequired(Signature.fromProtoV0, "signature", signature)
      domainUid <- UniqueIdentifier.fromProtoPrimitive(domainId, "domainId")
      notSequencedAfter <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "not_sequenced_after",
        timestamp,
      )
    } yield DomainTopologyTransactionMessage(
      signature,
      succeededContent,
      notSequencedAfter = Some(notSequencedAfter),
      DomainId(domainUid),
    )(protocolVersionRepresentativeFor(ProtoVersion(1)))
  }

  override def name: String = "DomainTopologyTransactionMessage"
}

final case class TopologyTransactionsBroadcastX private (
    override val domainId: DomainId,
    broadcasts: Seq[Broadcast],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TopologyTransactionsBroadcastX.type
    ]
) extends UnsignedProtocolMessage
    with UnsignedProtocolMessageV3 {

  @transient override protected lazy val companionObj: TopologyTransactionsBroadcastX.type =
    TopologyTransactionsBroadcastX

  override protected[messages] def toProtoSomeEnvelopeContentV3
      : v3.EnvelopeContent.SomeEnvelopeContent =
    v3.EnvelopeContent.SomeEnvelopeContent.TopologyTransactionsBroadcast(toProtoV2)

  def toProtoV2: v2.TopologyTransactionsBroadcastX = v2.TopologyTransactionsBroadcastX(
    domainId.toProtoPrimitive,
    broadcasts = broadcasts.map(_.toProtoV2),
  )

}

object TopologyTransactionsBroadcastX
    extends HasProtocolVersionedCompanion[
      TopologyTransactionsBroadcastX
    ] {

  def create(
      domainId: DomainId,
      broadcasts: Seq[Broadcast],
      protocolVersion: ProtocolVersion,
  ): TopologyTransactionsBroadcastX =
    TopologyTransactionsBroadcastX(domainId = domainId, broadcasts = broadcasts)(
      supportedProtoVersions.protocolVersionRepresentativeFor(protocolVersion)
    )

  override def name: String = "TopologyTransactionsBroadcastX"

  implicit val acceptedTopologyTransactionXMessageCast
      : ProtocolMessageContentCast[TopologyTransactionsBroadcastX] =
    ProtocolMessageContentCast.create[TopologyTransactionsBroadcastX](
      name
    ) {
      case att: TopologyTransactionsBroadcastX => Some(att)
      case _ => None
    }

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(-1) -> UnsupportedProtoCodec(ProtocolVersion.minimum),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.CNTestNet)(
      v2.TopologyTransactionsBroadcastX
    )(
      supportedProtoVersion(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
  )

  private[messages] def fromProtoV2(
      message: v2.TopologyTransactionsBroadcastX
  ): ParsingResult[TopologyTransactionsBroadcastX] = {
    val v2.TopologyTransactionsBroadcastX(domain, broadcasts) = message
    for {
      domainId <- DomainId.fromProtoPrimitive(domain, "domain")
      broadcasts <- broadcasts.traverse(broadcastFromProtoV2)
    } yield TopologyTransactionsBroadcastX(domainId, broadcasts.toList)(
      protocolVersionRepresentativeFor(ProtoVersion(2))
    )
  }

  private def broadcastFromProtoV2(
      message: v2.TopologyTransactionsBroadcastX.Broadcast
  ): ParsingResult[Broadcast] = {
    val v2.TopologyTransactionsBroadcastX.Broadcast(broadcastId, transactions) = message
    for {
      broadcastId <- String255.fromProtoPrimitive(broadcastId, "broadcast_id")
      transactions <- transactions.traverse(SignedTopologyTransactionX.fromProtoV2)
    } yield Broadcast(broadcastId, transactions.toList)
  }

  final case class Broadcast(
      broadcastId: TopologyRequestId,
      transactions: List[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
  ) {
    def toProtoV2: v2.TopologyTransactionsBroadcastX.Broadcast =
      v2.TopologyTransactionsBroadcastX.Broadcast(
        broadcastId = broadcastId.toProtoPrimitive,
        transactions = transactions.map(_.toProtoV2),
      )
  }

  /** The state of the submission of a topology transaction broadcast. In combination with the sequencer client
    * send tracker capability, State reflects that either the sequencer Accepted the submission or that the submission
    * was Rejected due to an error or a timeout. See DomainTopologyServiceX.
    */
  sealed trait State extends Product with Serializable

  object State {
    case object Failed extends State

    case object Accepted extends State
  }
}
