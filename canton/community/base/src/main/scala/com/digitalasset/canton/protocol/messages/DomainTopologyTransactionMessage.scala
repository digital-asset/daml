// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX.Broadcast
import com.digitalasset.canton.protocol.{v0, v1, v2, v3, v4}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
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
    with ProtocolMessageV3
    with UnsignedProtocolMessageV4 {

  // Ensures the invariants related to default values hold
  DomainTopologyTransactionMessage
    .validateInstance(this, representativeProtocolVersion)
    .valueOr(err => throw new IllegalArgumentException(err))

  def hashToSign(hashOps: HashOps): Hash =
    DomainTopologyTransactionMessage.hash(
      transactions,
      domainId,
      notSequencedAfter,
      hashOps,
      representativeProtocolVersion,
    )

  private[messages] def toProtoV0: v0.DomainTopologyTransactionMessage =
    v0.DomainTopologyTransactionMessage(
      signature = Some(domainTopologyManagerSignature.toProtoV0),
      transactions = transactions.map(_.getCryptographicEvidence),
      domainId = domainId.toProtoPrimitive,
    )

  private[messages] def toProtoV1: v1.DomainTopologyTransactionMessage =
    v1.DomainTopologyTransactionMessage(
      signature = Some(domainTopologyManagerSignature.toProtoV0),
      transactions = transactions.map(_.getCryptographicEvidence),
      domainId = domainId.toProtoPrimitive,
      notSequencedAfter = notSequencedAfter.map(_.toProtoPrimitive),
    )

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

  override def toProtoEnvelopeContentV3: v3.EnvelopeContent =
    v3.EnvelopeContent(
      v3.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV1)
    )

  override def toProtoSomeEnvelopeContentV4: v4.EnvelopeContent.SomeEnvelopeContent =
    v4.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV1)

  @transient override protected lazy val companionObj: DomainTopologyTransactionMessage.type =
    DomainTopologyTransactionMessage

  @VisibleForTesting
  def replaceSignatureForTesting(signature: Signature): DomainTopologyTransactionMessage = {
    copy(domainTopologyManagerSignature = signature)(representativeProtocolVersion)
  }

}

object DomainTopologyTransactionMessage
    extends HasProtocolVersionedWithContextCompanion[
      DomainTopologyTransactionMessage,
      ProtocolVersion,
    ] {

  implicit val domainIdentityTransactionMessageCast
      : ProtocolMessageContentCast[DomainTopologyTransactionMessage] =
    ProtocolMessageContentCast.create[DomainTopologyTransactionMessage](
      "DomainTopologyTransactionMessage"
    ) {
      case dttm: DomainTopologyTransactionMessage => Some(dttm)
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
      supportedProtoVersion(_)(fromProtoV1(ProtoVersion(1))),
      _.toProtoV1.toByteString,
    ),
    // use separate ProtoVersion starting with v6 to allow different hashing
    // scheme for protocol version 5 and 6
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v6)(
      v1.DomainTopologyTransactionMessage
    )(
      supportedProtoVersion(_)(fromProtoV1(ProtoVersion(2))),
      _.toProtoV1.toByteString,
    ),
  )

  override lazy val invariants = Seq(notSequencedAfterInvariant)
  lazy val notSequencedAfterInvariant = EmptyOptionExactlyUntilExclusive(
    _.notSequencedAfter,
    "notSequencedAfter",
    protocolVersionRepresentativeFor(ProtocolVersion.v5),
  )

  private def hash(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      domainId: DomainId,
      notSequencedAfter: Option[CantonTimestamp],
      hashOps: HashOps,
      representativeProtocolVersion: RepresentativeProtocolVersion[
        DomainTopologyTransactionMessage.type
      ],
  ): Hash = {
    val builder = hashOps
      .build(HashPurpose.DomainTopologyTransactionMessageSignature)
      .add(domainId.toProtoPrimitive)
    val version = representativeProtocolVersion.representative
    notSequencedAfter match {
      case Some(ts) if version == ProtocolVersion.v5 =>
        builder.add(ts.toEpochMilli).discard
      case Some(ts) if version >= ProtocolVersion.v6 =>
        builder
          .add(version.toProtoPrimitive)
          .add(ts.toMicros)
          .add(transactions.length)
          .discard
      case Some(_) =>
        throw new IllegalStateException(
          "notSequencedAfter not expected for pv < 5"
        )
      case None if version >= ProtocolVersion.v5 =>
        // Won't happen because of the invariant
        throw new IllegalStateException(
          "notSequencedAfter must be present for protocol version >= 5"
        )
      case None =>
    }

    transactions.foreach(elem => builder.add(elem.getCryptographicEvidence))
    builder.finish()

  }

  def create(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      syncCrypto: DomainSnapshotSyncCryptoApi,
      domainId: DomainId,
      notSequencedAfter: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, String, DomainTopologyTransactionMessage] = {
    val notSequencedAfterUpdated =
      notSequencedAfterInvariant.orValue(notSequencedAfter, protocolVersion)

    val representativeProtocolVersion = protocolVersionRepresentativeFor(protocolVersion)
    val hashToSign = hash(
      transactions,
      domainId,
      notSequencedAfterUpdated,
      syncCrypto.crypto.pureCrypto,
      representativeProtocolVersion,
    )
    for {

      signature <- syncCrypto.sign(hashToSign).leftMap(_.toString)
      domainTopologyTransactionMessageE = Either
        .catchOnly[IllegalArgumentException](
          DomainTopologyTransactionMessage(
            signature,
            transactions,
            notSequencedAfter = notSequencedAfterUpdated,
            domainId,
          )(representativeProtocolVersion)
        )
        .leftMap(_.getMessage)
      domainTopologyTransactionMessage <- EitherT.fromEither[Future](
        domainTopologyTransactionMessageE
      )
    } yield domainTopologyTransactionMessage
  }

  def tryCreate(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      crypto: DomainSnapshotSyncCryptoApi,
      domainId: DomainId,
      notSequencedAfter: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[DomainTopologyTransactionMessage] = {
    val notSequencedAfterUpdated =
      notSequencedAfterInvariant.orValue(notSequencedAfter, protocolVersion)

    create(transactions, crypto, domainId, notSequencedAfterUpdated, protocolVersion).fold(
      err =>
        throw new IllegalStateException(
          s"Failed to create domain topology transaction message: $err"
        ),
      identity,
    )
  }

  private[messages] def fromProtoV0(
      expectedProtocolVersion: ProtocolVersion,
      message: v0.DomainTopologyTransactionMessage,
  ): ParsingResult[DomainTopologyTransactionMessage] = {
    val v0.DomainTopologyTransactionMessage(signature, _domainId, transactions) = message

    for {
      succeededContent <- transactions.toList.traverse(
        SignedTopologyTransaction.fromByteString(
          ProtocolVersionValidation(expectedProtocolVersion)
        )
      )
      signature <- ProtoConverter.parseRequired(Signature.fromProtoV0, "signature", signature)
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
    } yield DomainTopologyTransactionMessage(
      signature,
      succeededContent,
      notSequencedAfter = None,
      DomainId(domainUid),
    )(protocolVersionRepresentativeFor(ProtoVersion(0)))
  }

  private[messages] def fromProtoV1(protoVersion: ProtoVersion)(
      expectedProtocolVersion: ProtocolVersion,
      message: v1.DomainTopologyTransactionMessage,
  ): ParsingResult[DomainTopologyTransactionMessage] = {
    val v1.DomainTopologyTransactionMessage(signature, domainId, timestamp, transactions) = message
    for {
      succeededContent <- transactions.toList.traverse(
        SignedTopologyTransaction.fromByteString(
          ProtocolVersionValidation(expectedProtocolVersion)
        )
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
    )(protocolVersionRepresentativeFor(protoVersion))
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
    with UnsignedProtocolMessageV4 {

  @transient override protected lazy val companionObj: TopologyTransactionsBroadcastX.type =
    TopologyTransactionsBroadcastX

  override protected[messages] def toProtoSomeEnvelopeContentV4
      : v4.EnvelopeContent.SomeEnvelopeContent =
    v4.EnvelopeContent.SomeEnvelopeContent.TopologyTransactionsBroadcast(toProtoV2)

  def toProtoV2: v2.TopologyTransactionsBroadcastX = v2.TopologyTransactionsBroadcastX(
    domainId.toProtoPrimitive,
    broadcasts = broadcasts.map(_.toProtoV2),
  )

}

object TopologyTransactionsBroadcastX
    extends HasProtocolVersionedWithContextCompanion[
      TopologyTransactionsBroadcastX,
      ProtocolVersion,
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
      expectedProtocolVersion: ProtocolVersion,
      message: v2.TopologyTransactionsBroadcastX,
  ): ParsingResult[TopologyTransactionsBroadcastX] = {
    val v2.TopologyTransactionsBroadcastX(domain, broadcasts) = message
    for {
      domainId <- DomainId.fromProtoPrimitive(domain, "domain")
      broadcasts <- broadcasts.traverse(broadcastFromProtoV2(expectedProtocolVersion))
    } yield TopologyTransactionsBroadcastX(domainId, broadcasts.toList)(
      protocolVersionRepresentativeFor(ProtoVersion(2))
    )
  }

  private def broadcastFromProtoV2(expectedProtocolVersion: ProtocolVersion)(
      message: v2.TopologyTransactionsBroadcastX.Broadcast
  ): ParsingResult[Broadcast] = {
    val v2.TopologyTransactionsBroadcastX.Broadcast(broadcastId, transactions) = message
    for {
      broadcastId <- String255.fromProtoPrimitive(broadcastId, "broadcast_id")
      transactions <- transactions.traverse(tx =>
        SignedTopologyTransactionX.fromProtoV2(
          ProtocolVersionValidation(expectedProtocolVersion),
          tx,
        )
      )
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
