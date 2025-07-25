// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Applicative
import com.digitalasset.canton.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, ProtocolMessage}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.sequencing.{EnvelopeBox, RawSignedContentEnvelopeBox}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionMemoization2,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import pprint.Tree
import pprint.Tree.{Apply, KeyValue, Literal}

/** The Deliver events are received as a consequence of a '''Send''' command, received by the
  * recipients of the originating '''Send''' event.
  */
sealed trait SequencedEvent[+Env <: Envelope[?]]
    extends Product
    with Serializable
    with ProtocolVersionedMemoizedEvidence
    with PrettyPrinting
    with HasProtocolVersionedWrapper[SequencedEvent[Envelope[?]]] {

  @transient override protected lazy val companionObj: SequencedEvent.type = SequencedEvent

  protected def toProtoV30: v30.SequencedEvent

  /** The timestamp of the previous event in the member's subscription, or `None` if this event is
    * the first
    */
  val previousTimestamp: Option[CantonTimestamp]

  /** a timestamp defining the order (requestId)
    */
  val timestamp: CantonTimestamp

  /** The synchronizer which this deliver event belongs to */
  val synchronizerId: PhysicalSynchronizerId

  protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  protected def traverse[F[_], Env2 <: Envelope[?]](f: Env => F[Env2])(implicit
      F: Applicative[F]
  ): F[SequencedEvent[Env2]]

  def envelopes: Seq[Env]

  /** The timestamp when the sequencer's signature key on the event's signature must be valid */
  def timestampOfSigningKey: CantonTimestamp
}

object SequencedEvent
    extends VersioningCompanionMemoization2[
      SequencedEvent[Envelope[?]],
      SequencedEvent[ClosedEnvelope],
    ] {
  override def name: String = "SequencedEvent"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.SequencedEvent)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private[sequencing] def fromProtoV30(sequencedEventP: v30.SequencedEvent)(
      bytes: ByteString
  ): ParsingResult[SequencedEvent[ClosedEnvelope]] = {
    import cats.syntax.traverse.*
    val v30.SequencedEvent(
      previousTimestampP,
      tsP,
      synchronizerIdP,
      mbMsgIdP,
      mbBatchP,
      mbDeliverErrorReasonP,
      topologyTimestampP,
      trafficConsumedP,
    ) = sequencedEventP

    for {
      previousTimestamp <- previousTimestampP.traverse(CantonTimestamp.fromProtoPrimitive)
      timestamp <- CantonTimestamp.fromProtoPrimitive(tsP)
      psid <- PhysicalSynchronizerId.fromProtoPrimitive(
        synchronizerIdP,
        "SequencedEvent.physical_synchronizer_id",
      )
      mbBatch <- mbBatchP.traverse(
        // TODO(i10428) Prevent zip bombing when decompressing the request
        Batch.fromProtoV30(_, maxRequestSize = MaxRequestSizeToDeserialize.NoLimit)
      )
      trafficConsumed <- trafficConsumedP.traverse(TrafficReceipt.fromProtoV30)
      // errors have an error reason, delivers have a batch
      event <- ((mbDeliverErrorReasonP, mbBatch) match {
        case (Some(_), Some(_)) =>
          Left(OtherError("SequencedEvent cannot have both a deliver error and batch set"))
        case (None, None) =>
          Left(OtherError("SequencedEvent cannot have neither a deliver error nor a batch set"))
        case (Some(deliverErrorReason), None) =>
          for {
            msgId <- ProtoConverter
              .required("DeliverError", mbMsgIdP)
              .flatMap(MessageId.fromProtoPrimitive)
            _ <- Either.cond(
              topologyTimestampP.isEmpty,
              (),
              OtherError("topology_timestamp must not be set for DeliverError"),
            )
          } yield new DeliverError(
            previousTimestamp,
            timestamp,
            psid,
            msgId,
            deliverErrorReason,
            trafficConsumed,
          )(Some(bytes)) {}
        case (None, Some(batch)) =>
          for {
            topologyTimestampO <- topologyTimestampP.traverse(CantonTimestamp.fromProtoPrimitive)
            msgIdO <- mbMsgIdP.traverse(MessageId.fromProtoPrimitive)
          } yield Deliver(
            previousTimestamp,
            timestamp,
            psid,
            msgIdO,
            batch,
            topologyTimestampO,
            trafficConsumed,
          )(
            Some(bytes)
          )
      }): ParsingResult[SequencedEvent[ClosedEnvelope]]
    } yield event
  }

  def fromByteStringOpen(hashOps: HashOps, protocolVersion: ProtocolVersion)(
      bytes: ByteString
  ): ParsingResult[SequencedEvent[DefaultOpenEnvelope]] =
    fromTrustedByteString(bytes).flatMap(
      _.traverse(_.openEnvelope(hashOps, protocolVersion))
    )

  implicit val sequencedEventEnvelopeBox: EnvelopeBox[SequencedEvent] =
    new EnvelopeBox[SequencedEvent] {
      override private[sequencing] def traverse[G[_], A <: Envelope[_], B <: Envelope[_]](
          event: SequencedEvent[A]
      )(f: A => G[B])(implicit G: Applicative[G]): G[SequencedEvent[B]] =
        event.traverse(f)
    }

  // It would be nice if we could appeal to a generic composition theorem here,
  // but the `MemoizeEvidence` bound in `SignedContent` doesn't allow a generic `Traverse` instance.
  implicit val signedContentEnvelopeBox: EnvelopeBox[RawSignedContentEnvelopeBox] =
    new EnvelopeBox[RawSignedContentEnvelopeBox] {
      override private[sequencing] def traverse[G[_], Env1 <: Envelope[_], Env2 <: Envelope[_]](
          signedEvent: SignedContent[SequencedEvent[Env1]]
      )(f: Env1 => G[Env2])(implicit G: Applicative[G]): G[RawSignedContentEnvelopeBox[Env2]] =
        signedEvent.traverse(_.traverse(f))
    }

  def openEnvelopes(
      event: SequencedEvent[ClosedEnvelope]
  )(protocolVersion: ProtocolVersion, hashOps: HashOps): (
      SequencedEvent[OpenEnvelope[ProtocolMessage]],
      Seq[ProtoDeserializationError],
  ) = event match {
    case deliver: Deliver[ClosedEnvelope] =>
      Deliver.openEnvelopes(deliver)(protocolVersion, hashOps)
    case deliver: DeliverError => (deliver, Seq.empty)
  }
}

sealed abstract case class DeliverError private[sequencing] (
    override val previousTimestamp: Option[CantonTimestamp],
    override val timestamp: CantonTimestamp,
    override val synchronizerId: PhysicalSynchronizerId,
    messageId: MessageId,
    reason: Status,
    trafficReceipt: Option[TrafficReceipt],
)(override val deserializedFrom: Option[ByteString])
    extends SequencedEvent[Nothing]
    with NoCopy {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[SequencedEvent.type] =
    SequencedEvent.protocolVersionRepresentativeFor(synchronizerId.protocolVersion)

  def toProtoV30: v30.SequencedEvent = v30.SequencedEvent(
    previousTimestamp = previousTimestamp.map(_.toProtoPrimitive),
    timestamp = timestamp.toProtoPrimitive,
    physicalSynchronizerId = synchronizerId.toProtoPrimitive,
    messageId = Some(messageId.toProtoPrimitive),
    batch = None,
    deliverErrorReason = Some(reason),
    topologyTimestamp = None,
    trafficReceipt = trafficReceipt.map(_.toProtoV30),
  )

  def updateTrafficReceipt(trafficReceipt: Option[TrafficReceipt]): DeliverError = new DeliverError(
    previousTimestamp,
    timestamp,
    synchronizerId,
    messageId,
    reason,
    trafficReceipt,
  )(deserializedFrom) {}

  override protected def traverse[F[_], Env <: Envelope[?]](f: Nothing => F[Env])(implicit
      F: Applicative[F]
  ): F[SequencedEvent[Env]] = F.pure(this)

  override protected def pretty: Pretty[DeliverError] = prettyOfClass(
    param("previous timestamp", _.previousTimestamp),
    param("timestamp", _.timestamp),
    param("id", _.synchronizerId),
    param("message id", _.messageId),
    param("reason", _.reason),
    paramIfDefined("traffic receipt", _.trafficReceipt),
  )

  def envelopes: Seq[Nothing] = Seq.empty

  override def timestampOfSigningKey: CantonTimestamp = timestamp
}

object DeliverError {

  implicit val prettyStatus: Pretty[Status] = new Pretty[Status] {
    override def treeOf(t: Status): Tree =
      Apply(
        "Status",
        Seq(
          KeyValue("Code", Literal(t.code.toString)),
          KeyValue("Message", Literal(t.message)),
        ).iterator,
      )
  }

  def create(
      previousTimestamp: Option[CantonTimestamp],
      timestamp: CantonTimestamp,
      synchronizerId: PhysicalSynchronizerId,
      messageId: MessageId,
      sequencerError: SequencerDeliverError,
      trafficReceipt: Option[TrafficReceipt],
  ): DeliverError =
    new DeliverError(
      previousTimestamp,
      timestamp,
      synchronizerId,
      messageId,
      sequencerError.rpcStatusWithoutLoggingContext(),
      trafficReceipt,
    )(None) {}
  def create(
      previousTimestamp: Option[CantonTimestamp],
      timestamp: CantonTimestamp,
      synchronizerId: PhysicalSynchronizerId,
      messageId: MessageId,
      status: Status,
      trafficReceipt: Option[TrafficReceipt],
  ): DeliverError =
    new DeliverError(
      previousTimestamp,
      timestamp,
      synchronizerId,
      messageId,
      status,
      trafficReceipt,
    )(None) {}
}

/** Intuitively, the member learns all envelopes addressed to it. It learns some recipients of these
  * envelopes, as defined by [[com.digitalasset.canton.sequencing.protocol.Recipients.forMember]]
  *
  * @param previousTimestamp
  *   a timestamp of the previous event in the member's subscription, or `None` if this event is the
  *   first
  * @param timestamp
  *   a timestamp defining the order.
  * @param messageIdO
  *   populated with the message id used on the originating send operation only for the sender
  * @param batch
  *   a batch of envelopes.
  * @param topologyTimestampO
  *   the timestamp of the topology snapshot used for resolving group recipients if this timestamp
  *   is different from the sequencing `timestamp`
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class Deliver[+Env <: Envelope[_]] private[sequencing] (
    override val previousTimestamp: Option[CantonTimestamp],
    override val timestamp: CantonTimestamp,
    override val synchronizerId: PhysicalSynchronizerId,
    messageIdO: Option[MessageId],
    batch: Batch[Env],
    topologyTimestampO: Option[CantonTimestamp],
    trafficReceipt: Option[TrafficReceipt],
)(
    val deserializedFrom: Option[ByteString]
) extends SequencedEvent[Env] {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[SequencedEvent.type] =
    SequencedEvent.protocolVersionRepresentativeFor(synchronizerId.protocolVersion)

  /** Is this deliver event a receipt for a message that the receiver previously sent? (messageId is
    * only populated for the sender)
    */
  lazy val isReceipt: Boolean = messageIdO.isDefined

  protected[sequencing] def toProtoV30: v30.SequencedEvent = v30.SequencedEvent(
    previousTimestamp = previousTimestamp.map(_.toProtoPrimitive),
    timestamp = timestamp.toProtoPrimitive,
    physicalSynchronizerId = synchronizerId.toProtoPrimitive,
    messageId = messageIdO.map(_.toProtoPrimitive),
    batch = Some(batch.toProtoV30),
    deliverErrorReason = None,
    topologyTimestamp = topologyTimestampO.map(_.toProtoPrimitive),
    trafficReceipt = trafficReceipt.map(_.toProtoV30),
  )

  protected def traverse[F[_], Env2 <: Envelope[?]](
      f: Env => F[Env2]
  )(implicit F: Applicative[F]): F[SequencedEvent[Env2]] =
    F.map(batch.traverse(f))(
      Deliver(
        previousTimestamp,
        timestamp,
        synchronizerId,
        messageIdO,
        _,
        topologyTimestampO,
        trafficReceipt,
      )(deserializedFrom)
    )

  @VisibleForTesting
  private[canton] def copy[Env2 <: Envelope[?]](
      previousTimestamp: Option[CantonTimestamp] = this.previousTimestamp,
      timestamp: CantonTimestamp = this.timestamp,
      synchronizerId: PhysicalSynchronizerId = this.synchronizerId,
      messageIdO: Option[MessageId] = this.messageIdO,
      batch: Batch[Env2] = this.batch,
      topologyTimestampO: Option[CantonTimestamp] = this.topologyTimestampO,
      deserializedFromO: Option[ByteString] = None,
      trafficReceipt: Option[TrafficReceipt] = this.trafficReceipt,
  ): Deliver[Env2] =
    Deliver[Env2](
      previousTimestamp,
      timestamp,
      synchronizerId,
      messageIdO,
      batch,
      topologyTimestampO,
      trafficReceipt,
    )(deserializedFromO)

  def updateTrafficReceipt(trafficReceipt: Option[TrafficReceipt]): Deliver[Env] =
    copy(trafficReceipt = trafficReceipt)

  override protected def pretty: Pretty[this.type] =
    prettyOfClass(
      param("previous timestamp", _.previousTimestamp),
      param("timestamp", _.timestamp),
      paramIfNonEmpty("message id", _.messageIdO),
      param("synchronizer id", _.synchronizerId),
      paramIfDefined("topology timestamp", _.topologyTimestampO),
      unnamedParam(_.batch),
      paramIfDefined("traffic receipt", _.trafficReceipt),
    )

  def envelopes: Seq[Env] = batch.envelopes

  override def timestampOfSigningKey: CantonTimestamp = topologyTimestampO.getOrElse(timestamp)
}

object Deliver {
  def create[Env <: Envelope[_]](
      previousTimestamp: Option[CantonTimestamp],
      timestamp: CantonTimestamp,
      synchronizerId: PhysicalSynchronizerId,
      messageIdO: Option[MessageId],
      batch: Batch[Env],
      topologyTimestampO: Option[CantonTimestamp],
      trafficReceipt: Option[TrafficReceipt],
  ): Deliver[Env] =
    Deliver[Env](
      previousTimestamp,
      timestamp,
      synchronizerId,
      messageIdO,
      batch,
      topologyTimestampO,
      trafficReceipt,
    )(None)

  def fromSequencedEvent[Env <: Envelope[_]](
      deliverEvent: SequencedEvent[Env]
  ): Option[Deliver[Env]] =
    deliverEvent match {
      case deliver @ Deliver(_, _, _, _, _, _, _) => Some(deliver)
      case _: DeliverError => None
    }

  def openEnvelopes(
      deliver: Deliver[ClosedEnvelope]
  )(protocolVersion: ProtocolVersion, hashOps: HashOps): (
      Deliver[OpenEnvelope[ProtocolMessage]],
      Seq[ProtoDeserializationError],
  ) = {
    val (openBatch, openingErrors) =
      Batch.openEnvelopes(deliver.batch)(protocolVersion, hashOps)
    val openDeliver = deliver.copy(
      batch = openBatch,
      // Keep the serialized representation only if there were no errors
      deserializedFromO = if (openingErrors.isEmpty) deliver.deserializedFrom else None,
    )

    (openDeliver, openingErrors)
  }
}
