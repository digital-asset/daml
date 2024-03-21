// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.{InvariantViolation, NonNegativeInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** @param aggregationRule If [[scala.Some$]], this submission request is aggregatable.
  *                        Its envelopes will be delivered only when the rule's conditions are met.
  *                        The receipt of delivery for an aggregatable submission will be delivered immediately to the sender
  *                        even if the rule's conditions are not met.
  */
final case class SubmissionRequest private (
    sender: Member,
    messageId: MessageId,
    isRequest: Boolean,
    batch: Batch[ClosedEnvelope],
    maxSequencingTime: CantonTimestamp,
    timestampOfSigningKey: Option[CantonTimestamp],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SubmissionRequest.type
    ],
    override val deserializedFrom: Option[ByteString] = None,
) extends HasProtocolVersionedWrapper[SubmissionRequest]
    with ProtocolVersionedMemoizedEvidence {
  // Ensures the invariants related to default values hold
  validateInstance().valueOr(err => throw new IllegalArgumentException(err))

  private lazy val batchProtoV0: v0.CompressedBatch = batch.toProtoV0

  @transient override protected lazy val companionObj: SubmissionRequest.type = SubmissionRequest

  // Caches the serialized request to be able to do checks on its size without re-serializing
  lazy val toProtoV0: v0.SubmissionRequest = v0.SubmissionRequest(
    sender = sender.toProtoPrimitive,
    messageId = messageId.toProtoPrimitive,
    isRequest = isRequest,
    batch = Some(batchProtoV0),
    maxSequencingTime = Some(maxSequencingTime.toProtoPrimitive),
    timestampOfSigningKey = timestampOfSigningKey.map(_.toProtoPrimitive),
  )

  @VisibleForTesting
  def copy(
      sender: Member = this.sender,
      messageId: MessageId = this.messageId,
      isRequest: Boolean = this.isRequest,
      batch: Batch[ClosedEnvelope] = this.batch,
      maxSequencingTime: CantonTimestamp = this.maxSequencingTime,
      timestampOfSigningKey: Option[CantonTimestamp] = this.timestampOfSigningKey,
  ) = SubmissionRequest
    .create(
      sender,
      messageId,
      isRequest,
      batch,
      maxSequencingTime,
      timestampOfSigningKey,
      representativeProtocolVersion,
    )
    .valueOr(err => throw new IllegalArgumentException(err.message))

  def isConfirmationRequest(mediator: Member): Boolean =
    batch.envelopes.exists(
      _.recipients.allRecipients.forgetNE == Set(Recipient(mediator))
    ) && batch.envelopes.exists(e =>
      e.recipients.allRecipients.forgetNE != Set(Recipient(mediator))
    )

  def isConfirmationResponse(mediator: Member): Boolean =
    batch.envelopes.nonEmpty && batch.envelopes.forall(
      _.recipients.allRecipients.forgetNE == Set(Recipient(mediator))
    )

  def isMediatorResult(mediator: Member): Boolean = batch.envelopes.nonEmpty && sender == mediator

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString
}
sealed trait MaxRequestSizeToDeserialize {
  val toOption: Option[NonNegativeInt] = this match {
    case MaxRequestSizeToDeserialize.Limit(value) => Some(value)
    case MaxRequestSizeToDeserialize.NoLimit => None
  }
}
object MaxRequestSizeToDeserialize {
  final case class Limit(value: NonNegativeInt) extends MaxRequestSizeToDeserialize
  case object NoLimit extends MaxRequestSizeToDeserialize
}

object SubmissionRequest
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      SubmissionRequest,
      MaxRequestSizeToDeserialize,
    ] {
  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.SubmissionRequest)(
      supportedProtoVersionMemoized(_) { (maxRequestSize, req) => bytes =>
        fromProtoV0(maxRequestSize)(req, Some(bytes))
      },
      _.toProtoV0.toByteString,
    )
  )

  override def name: String = "submission request"

  def create(
      sender: Member,
      messageId: MessageId,
      isRequest: Boolean,
      batch: Batch[ClosedEnvelope],
      maxSequencingTime: CantonTimestamp,
      timestampOfSigningKey: Option[CantonTimestamp],
      representativeProtocolVersion: RepresentativeProtocolVersion[SubmissionRequest.type],
  ): Either[InvariantViolation, SubmissionRequest] =
    Either
      .catchOnly[IllegalArgumentException](
        new SubmissionRequest(
          sender,
          messageId,
          isRequest,
          batch,
          maxSequencingTime,
          timestampOfSigningKey,
        )(representativeProtocolVersion, deserializedFrom = None)
      )
      .leftMap(error => InvariantViolation(error.getMessage))

  def tryCreate(
      sender: Member,
      messageId: MessageId,
      isRequest: Boolean,
      batch: Batch[ClosedEnvelope],
      maxSequencingTime: CantonTimestamp,
      timestampOfSigningKey: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): SubmissionRequest =
    create(
      sender,
      messageId,
      isRequest,
      batch,
      maxSequencingTime,
      timestampOfSigningKey,
      protocolVersionRepresentativeFor(protocolVersion),
    ).valueOr(err => throw new IllegalArgumentException(err.message))

  def fromProtoV0(
      requestP: v0.SubmissionRequest,
      maxRequestSize: MaxRequestSizeToDeserialize,
  ): ParsingResult[SubmissionRequest] =
    fromProtoV0(maxRequestSize)(requestP, None)

  private def fromProtoV0(maxRequestSize: MaxRequestSizeToDeserialize)(
      requestP: v0.SubmissionRequest,
      bytes: Option[ByteString],
  ): ParsingResult[SubmissionRequest] = {
    val v0.SubmissionRequest(
      senderP,
      messageIdP,
      isRequest,
      batchP,
      maxSequencingTimeP,
      timestampOfSigningKey,
    ) = requestP

    for {
      sender <- Member.fromProtoPrimitive(senderP, "sender")
      messageId <- MessageId.fromProtoPrimitive(messageIdP)
      maxSequencingTime <- ProtoConverter
        .required("SubmissionRequest.maxSequencingTime", maxSequencingTimeP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
      batch <- ProtoConverter
        .required("SubmissionRequest.batch", batchP)
        .flatMap(Batch.fromProtoV0(_, maxRequestSize))
      ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(0))
    } yield new SubmissionRequest(
      sender,
      messageId,
      isRequest,
      batch,
      maxSequencingTime,
      ts,
    )(rpv, bytes)
  }

}
