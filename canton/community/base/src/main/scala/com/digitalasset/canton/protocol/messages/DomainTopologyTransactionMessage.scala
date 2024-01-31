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
import com.digitalasset.canton.protocol.v30
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
    notSequencedAfter: CantonTimestamp,
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      DomainTopologyTransactionMessage.type
    ]
) extends UnsignedProtocolMessage {

  def hashToSign(hashOps: HashOps): Hash =
    DomainTopologyTransactionMessage.hash(
      transactions,
      domainId,
      notSequencedAfter,
      hashOps,
    )

  private[messages] def toProtoV30: v30.DomainTopologyTransactionMessage =
    v30.DomainTopologyTransactionMessage(
      signature = Some(domainTopologyManagerSignature.toProtoV30),
      transactions = transactions.map(_.getCryptographicEvidence),
      domainId = domainId.toProtoPrimitive,
      notSequencedAfter = Some(notSequencedAfter.toProtoPrimitive),
    )

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.DomainTopologyTransactionMessage(toProtoV30)

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
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v30.DomainTopologyTransactionMessage
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private def hash(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      domainId: DomainId,
      notSequencedAfter: CantonTimestamp,
      hashOps: HashOps,
  ): Hash = {
    val builder = hashOps
      .build(HashPurpose.DomainTopologyTransactionMessageSignature)
      .add(domainId.toProtoPrimitive)

    builder.add(notSequencedAfter.toEpochMilli)

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
  ): EitherT[Future, String, DomainTopologyTransactionMessage] = {

    val hashToSign = hash(
      transactions,
      domainId,
      notSequencedAfter,
      syncCrypto.crypto.pureCrypto,
    )

    for {
      signature <- syncCrypto.sign(hashToSign).leftMap(_.toString)
      domainTopologyTransactionMessageE = Either
        .catchOnly[IllegalArgumentException](
          DomainTopologyTransactionMessage(
            signature,
            transactions,
            notSequencedAfter = notSequencedAfter,
            domainId,
          )(protocolVersionRepresentativeFor(protocolVersion))
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
      notSequencedAfter: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[DomainTopologyTransactionMessage] = {
    create(transactions, crypto, domainId, notSequencedAfter, protocolVersion).fold(
      err =>
        throw new IllegalStateException(
          s"Failed to create domain topology transaction message: $err"
        ),
      identity,
    )
  }

  private[messages] def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      message: v30.DomainTopologyTransactionMessage,
  ): ParsingResult[DomainTopologyTransactionMessage] = {
    val v30.DomainTopologyTransactionMessage(signature, domainId, timestamp, transactions) = message
    for {
      succeededContent <- transactions.toList.traverse(
        SignedTopologyTransaction.fromByteString(
          ProtocolVersionValidation(expectedProtocolVersion)
        )
      )
      signature <- ProtoConverter.parseRequired(Signature.fromProtoV30, "signature", signature)
      domainUid <- UniqueIdentifier.fromProtoPrimitive(domainId, "domainId")
      notSequencedAfter <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "not_sequenced_after",
        timestamp,
      )
    } yield DomainTopologyTransactionMessage(
      signature,
      succeededContent,
      notSequencedAfter = notSequencedAfter,
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
) extends UnsignedProtocolMessage {

  @transient override protected lazy val companionObj: TopologyTransactionsBroadcastX.type =
    TopologyTransactionsBroadcastX

  override protected[messages] def toProtoSomeEnvelopeContentV30
      : v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.TopologyTransactionsBroadcast(toProtoV30)

  def toProtoV30: v30.TopologyTransactionsBroadcastX = v30.TopologyTransactionsBroadcastX(
    domainId.toProtoPrimitive,
    broadcasts = broadcasts.map(_.toProtoV30),
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
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v30.TopologyTransactionsBroadcastX
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private[messages] def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      message: v30.TopologyTransactionsBroadcastX,
  ): ParsingResult[TopologyTransactionsBroadcastX] = {
    val v30.TopologyTransactionsBroadcastX(domain, broadcasts) = message
    for {
      domainId <- DomainId.fromProtoPrimitive(domain, "domain")
      broadcasts <- broadcasts.traverse(broadcastFromProtoV30(expectedProtocolVersion))
    } yield TopologyTransactionsBroadcastX(domainId, broadcasts.toList)(
      protocolVersionRepresentativeFor(ProtoVersion(2))
    )
  }

  private def broadcastFromProtoV30(expectedProtocolVersion: ProtocolVersion)(
      message: v30.TopologyTransactionsBroadcastX.Broadcast
  ): ParsingResult[Broadcast] = {
    val v30.TopologyTransactionsBroadcastX.Broadcast(broadcastId, transactions) = message
    for {
      broadcastId <- String255.fromProtoPrimitive(broadcastId, "broadcast_id")
      transactions <- transactions.traverse(tx =>
        SignedTopologyTransactionX.fromProtoV30(
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
    def toProtoV30: v30.TopologyTransactionsBroadcastX.Broadcast =
      v30.TopologyTransactionsBroadcastX.Broadcast(
        broadcastId = broadcastId.toProtoPrimitive,
        transactions = transactions.map(_.toProtoV30),
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
