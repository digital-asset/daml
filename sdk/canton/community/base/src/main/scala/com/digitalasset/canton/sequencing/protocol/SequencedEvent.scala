// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Applicative
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, ProtocolMessage}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencing.{EnvelopeBox, RawSignedContentEnvelopeBox}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWrapperCompanion2,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import pprint.Tree
import pprint.Tree.{Apply, KeyValue, Literal}

/** The Deliver events are received as a consequence of a '''Send''' command, received by the recipients of the
  * originating '''Send''' event.
  */
sealed trait SequencedEvent[+Env <: Envelope[?]]
    extends Product
    with Serializable
    with ProtocolVersionedMemoizedEvidence
    with PrettyPrinting
    with HasProtocolVersionedWrapper[SequencedEvent[Envelope[?]]] {

  @transient override protected lazy val companionObj: SequencedEvent.type = SequencedEvent

  protected def toProtoV30: v30.SequencedEvent

  /** a sequence counter for each recipient.
    */
  val counter: SequencerCounter

  /** a timestamp defining the order (requestId)
    */
  val timestamp: CantonTimestamp

  /** The domain which this deliver event belongs to */
  val domainId: DomainId

  def isTombstone: Boolean = false

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
    extends HasMemoizedProtocolVersionedWrapperCompanion2[
      SequencedEvent[Envelope[?]],
      SequencedEvent[ClosedEnvelope],
    ] {
  override def name: String = "SequencedEvent"

  override val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.SequencedEvent)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private[sequencing] def fromProtoV30(sequencedEventP: v30.SequencedEvent)(
      bytes: ByteString
  ): ParsingResult[SequencedEvent[ClosedEnvelope]] = {
    import cats.syntax.traverse.*
    val v30.SequencedEvent(
      counter,
      tsP,
      domainIdP,
      mbMsgIdP,
      mbBatchP,
      mbDeliverErrorReasonP,
      topologyTimestampP,
    ) = sequencedEventP

    val sequencerCounter = SequencerCounter(counter)

    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      timestamp <- CantonTimestamp.fromProtoPrimitive(tsP)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "SequencedEvent.domainId")
      mbBatch <- mbBatchP.traverse(
        // TODO(i10428) Prevent zip bombing when decompressing the request
        Batch.fromProtoV30(_, maxRequestSize = MaxRequestSizeToDeserialize.NoLimit)
      )
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
            sequencerCounter,
            timestamp,
            domainId,
            msgId,
            deliverErrorReason,
          )(rpv, Some(bytes)) {}
        case (None, Some(batch)) =>
          for {
            topologyTimestampO <- topologyTimestampP.traverse(CantonTimestamp.fromProtoPrimitive)
            msgIdO <- mbMsgIdP.traverse(MessageId.fromProtoPrimitive)
          } yield Deliver(sequencerCounter, timestamp, domainId, msgIdO, batch, topologyTimestampO)(
            rpv,
            Some(bytes),
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
    override val counter: SequencerCounter,
    override val timestamp: CantonTimestamp,
    override val domainId: DomainId,
    messageId: MessageId,
    reason: Status,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[SequencedEvent.type],
    override val deserializedFrom: Option[ByteString],
) extends SequencedEvent[Nothing]
    with NoCopy {

  def toProtoV30: v30.SequencedEvent = v30.SequencedEvent(
    counter = counter.toProtoPrimitive,
    timestamp = timestamp.toProtoPrimitive,
    domainId = domainId.toProtoPrimitive,
    messageId = Some(messageId.toProtoPrimitive),
    batch = None,
    deliverErrorReason = Some(reason),
    topologyTimestamp = None,
  )

  override protected def traverse[F[_], Env <: Envelope[?]](f: Nothing => F[Env])(implicit
      F: Applicative[F]
  ): F[SequencedEvent[Env]] = F.pure(this)

  override def pretty: Pretty[DeliverError] = prettyOfClass(
    param("counter", _.counter),
    param("timestamp", _.timestamp),
    param("domain id", _.domainId),
    param("message id", _.messageId),
    param("reason", _.reason),
  )

  def envelopes: Seq[Nothing] = Seq.empty

  override def isTombstone: Boolean = reason match {
    case SequencerErrors.PersistTombstone(_) => true
    case _ => false
  }

  override def timestampOfSigningKey: CantonTimestamp = timestamp
}

object DeliverError {

  implicit val prettyStatus: Pretty[Status] = new Pretty[Status] {
    override def treeOf(t: Status): Tree = {
      Apply(
        "Status",
        Seq(
          KeyValue("Code", Literal(t.code.toString)),
          KeyValue("Message", Literal(t.message)),
        ).iterator,
      )
    }
  }

  def create(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      domainId: DomainId,
      messageId: MessageId,
      sequencerError: SequencerDeliverError,
      protocolVersion: ProtocolVersion,
  ): DeliverError = {
    new DeliverError(
      counter,
      timestamp,
      domainId,
      messageId,
      sequencerError.rpcStatusWithoutLoggingContext(),
    )(
      SequencedEvent.protocolVersionRepresentativeFor(protocolVersion),
      None,
    ) {}
  }

  def create(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      domainId: DomainId,
      messageId: MessageId,
      status: Status,
      protocolVersion: ProtocolVersion,
  ): DeliverError =
    new DeliverError(counter, timestamp, domainId, messageId, status)(
      SequencedEvent.protocolVersionRepresentativeFor(protocolVersion),
      None,
    ) {}
}

/** Intuitively, the member learns all envelopes addressed to it. It learns some recipients of
  * these envelopes, as defined by
  * [[com.digitalasset.canton.sequencing.protocol.Recipients.forMember]]
  *
  * @param counter   a monotonically increasing counter for each recipient.
  * @param timestamp a timestamp defining the order.
  * @param messageIdO  populated with the message id used on the originating send operation only for the sender
  * @param batch     a batch of envelopes.
  * @param topologyTimestampO the timestamp of the topology snapshot used for resolving group recipients if this timestamp is different from the sequencing `timestamp`
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class Deliver[+Env <: Envelope[_]] private[sequencing] (
    override val counter: SequencerCounter,
    override val timestamp: CantonTimestamp,
    override val domainId: DomainId,
    messageIdO: Option[MessageId],
    batch: Batch[Env],
    topologyTimestampO: Option[CantonTimestamp],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[SequencedEvent.type],
    val deserializedFrom: Option[ByteString],
) extends SequencedEvent[Env] {

  /** Is this deliver event a receipt for a message that the receiver previously sent?
    * (messageId is only populated for the sender)
    */
  lazy val isReceipt: Boolean = messageIdO.isDefined

  protected[sequencing] def toProtoV30: v30.SequencedEvent = v30.SequencedEvent(
    counter = counter.toProtoPrimitive,
    timestamp = timestamp.toProtoPrimitive,
    domainId = domainId.toProtoPrimitive,
    messageId = messageIdO.map(_.toProtoPrimitive),
    batch = Some(batch.toProtoV30),
    deliverErrorReason = None,
    topologyTimestamp = topologyTimestampO.map(_.toProtoPrimitive),
  )

  protected def traverse[F[_], Env2 <: Envelope[?]](
      f: Env => F[Env2]
  )(implicit F: Applicative[F]): F[SequencedEvent[Env2]] =
    F.map(batch.traverse(f))(
      Deliver(counter, timestamp, domainId, messageIdO, _, topologyTimestampO)(
        representativeProtocolVersion,
        deserializedFrom,
      )
    )

  @VisibleForTesting
  private[canton] def copy[Env2 <: Envelope[?]](
      counter: SequencerCounter = this.counter,
      timestamp: CantonTimestamp = this.timestamp,
      domainId: DomainId = this.domainId,
      messageIdO: Option[MessageId] = this.messageIdO,
      batch: Batch[Env2] = this.batch,
      topologyTimestampO: Option[CantonTimestamp] = this.topologyTimestampO,
      deserializedFromO: Option[ByteString] = None,
  ): Deliver[Env2] =
    Deliver[Env2](counter, timestamp, domainId, messageIdO, batch, topologyTimestampO)(
      representativeProtocolVersion,
      deserializedFromO,
    )

  override def pretty: Pretty[this.type] =
    prettyOfClass(
      param("counter", _.counter),
      param("timestamp", _.timestamp),
      paramIfNonEmpty("message id", _.messageIdO),
      param("domain id", _.domainId),
      paramIfDefined("topology timestamp", _.topologyTimestampO),
      unnamedParam(_.batch),
    )

  def envelopes: Seq[Env] = batch.envelopes

  override def timestampOfSigningKey: CantonTimestamp = topologyTimestampO.getOrElse(timestamp)
}

object Deliver {
  def create[Env <: Envelope[_]](
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      domainId: DomainId,
      messageIdO: Option[MessageId],
      batch: Batch[Env],
      topologyTimestampO: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): Deliver[Env] =
    Deliver[Env](counter, timestamp, domainId, messageIdO, batch, topologyTimestampO)(
      SequencedEvent.protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  def fromSequencedEvent[Env <: Envelope[_]](
      deliverEvent: SequencedEvent[Env]
  ): Option[Deliver[Env]] =
    deliverEvent match {
      case deliver @ Deliver(_, _, _, _, _, _) => Some(deliver)
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
