// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.{InvariantViolation, NonNegativeInt}
import com.digitalasset.canton.crypto.{HashOps, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencing.protocol
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  DeterministicEncoding,
  ProtoConverter,
  ProtocolVersionedMemoizedEvidence,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.util.EitherUtil
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
    aggregationRule: Option[AggregationRule],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SubmissionRequest.type
    ],
    override val deserializedFrom: Option[ByteString] = None,
) extends HasProtocolVersionedWrapper[SubmissionRequest]
    with ProtocolVersionedMemoizedEvidence {
  // Ensures the invariants related to default values hold
  validateInstance().valueOr(err => throw new IllegalArgumentException(err))

  @transient override protected lazy val companionObj: SubmissionRequest.type = SubmissionRequest

  // Caches the serialized request to be able to do checks on its size without re-serializing
  lazy val toProtoV30: v30.SubmissionRequest = v30.SubmissionRequest(
    sender = sender.toProtoPrimitive,
    messageId = messageId.toProtoPrimitive,
    isRequest = isRequest,
    batch = Some(batch.toProtoV30),
    maxSequencingTime = Some(maxSequencingTime.toProtoPrimitive),
    timestampOfSigningKey = timestampOfSigningKey.map(_.toProtoPrimitive),
    aggregationRule = aggregationRule.map(_.toProtoV30),
  )

  @VisibleForTesting
  def copy(
      sender: Member = this.sender,
      messageId: MessageId = this.messageId,
      isRequest: Boolean = this.isRequest,
      batch: Batch[ClosedEnvelope] = this.batch,
      maxSequencingTime: CantonTimestamp = this.maxSequencingTime,
      timestampOfSigningKey: Option[CantonTimestamp] = this.timestampOfSigningKey,
      aggregationRule: Option[AggregationRule] = this.aggregationRule,
  ) = SubmissionRequest
    .create(
      sender,
      messageId,
      isRequest,
      batch,
      maxSequencingTime,
      timestampOfSigningKey,
      aggregationRule,
      representativeProtocolVersion,
    )
    .valueOr(err => throw new IllegalArgumentException(err.message))

  def isConfirmationRequest(mediator: Member): Boolean =
    batch.envelopes.exists(
      _.recipients.allRecipients.forgetNE == Set(MemberRecipient(mediator))
    ) && batch.envelopes.exists(e =>
      e.recipients.allRecipients.forgetNE != Set(MemberRecipient(mediator))
    )

  def isConfirmationResponse(mediator: Member): Boolean =
    batch.envelopes.nonEmpty && batch.envelopes.forall(
      _.recipients.allRecipients.forgetNE == Set(MemberRecipient(mediator))
    )

  def isMediatorResult(mediator: Member): Boolean = batch.envelopes.nonEmpty && sender == mediator

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  /** Returns the [[AggregationId]] for grouping if this is an aggregatable submission.
    * The aggregation ID computationally authenticates the relevant contents of the submission request, namely,
    * <ul>
    *   <li>Envelope contents [[com.digitalasset.canton.sequencing.protocol.ClosedEnvelope.bytes]],
    *     the recipients [[com.digitalasset.canton.sequencing.protocol.ClosedEnvelope.recipients]] of the [[batch]],
    *     and whether there are signatures.
    *   <li>The [[maxSequencingTime]]</li>
    *   <li>The [[timestampOfSigningKey]]</li>
    *   <li>The [[aggregationRule]]</li>
    * </ul>
    *
    * The [[AggregationId]] does not authenticate the following pieces of a submission request:
    * <ul>
    *   <li>The signatures [[com.digitalasset.canton.sequencing.protocol.ClosedEnvelope.signatures]] on the closed envelopes
    *     because the signatures differ for each sender. Aggregating the signatures is the whole point of an aggregatable submission.
    *     In contrast, the presence of signatures is relevant for the ID because it determines how the
    *     [[com.digitalasset.canton.sequencing.protocol.ClosedEnvelope.bytes]] are interpreted.
    *     </li>
    *   <li>The [[sender]] and the [[messageId]], as they are specific to the sender of a particular submission request</li>
    *   <li>The [[isRequest]] flag because it is irrelevant for delivery or aggregation</li>
    * </ul>
    */
  def aggregationId(hashOps: HashOps): Option[AggregationId] = aggregationRule.map { rule =>
    val builder = hashOps.build(HashPurpose.AggregationId)
    builder.add(batch.envelopes.length)
    batch.envelopes.foreach { envelope =>
      val ClosedEnvelope(content, recipients, signatures) = envelope
      builder.add(DeterministicEncoding.encodeBytes(content))
      builder.add(
        DeterministicEncoding.encodeBytes(
          // TODO(#12075) Use a deterministic serialization scheme for the recipients
          recipients.toProtoV30.toByteString
        )
      )
      builder.add(DeterministicEncoding.encodeByte(if (signatures.isEmpty) 0x00 else 0x01))
    }
    builder.add(maxSequencingTime.underlying.micros)
    // CantonTimestamp's microseconds can never be Long.MinValue, so the encoding remains injective if we use Long.MaxValue as the default.
    builder.add(timestampOfSigningKey.fold(Long.MinValue)(_.underlying.micros))
    builder.add(rule.eligibleSenders.size)
    rule.eligibleSenders.foreach(member => builder.add(member.toProtoPrimitive))
    builder.add(rule.threshold.value)
    val hash = builder.finish()
    AggregationId(hash)
  }
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
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v30
    )(v30.SubmissionRequest)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  override def name: String = "submission request"

  override lazy val invariants: Seq[protocol.SubmissionRequest.Invariant] =
    Seq(aggregationRuleDefaultValue, timestampOfSigningKeyInvariant)

  lazy val aggregationRuleDefaultValue
      : SubmissionRequest.DefaultValueUntilExclusive[Option[AggregationRule]] =
    DefaultValueUntilExclusive(
      _.aggregationRule,
      "aggregationRule",
      protocolVersionRepresentativeFor(ProtoVersion(1)),
      None,
    )

  lazy val timestampOfSigningKeyInvariant = new Invariant {
    override def validateInstance(
        v: SubmissionRequest,
        rpv: SubmissionRequest.ThisRepresentativeProtocolVersion,
    ): Either[String, Unit] =
      EitherUtil.condUnitE(
        v.aggregationRule.isEmpty || v.timestampOfSigningKey.isDefined,
        s"Submission request has `aggregationRule` set, but `timestampOfSigningKey` is not defined. Please check that `timestampOfSigningKey` has been set for the submission.",
      )
  }

  def create(
      sender: Member,
      messageId: MessageId,
      isRequest: Boolean,
      batch: Batch[ClosedEnvelope],
      maxSequencingTime: CantonTimestamp,
      timestampOfSigningKey: Option[CantonTimestamp],
      aggregationRule: Option[AggregationRule],
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
          aggregationRule,
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
      aggregationRule: Option[AggregationRule],
      protocolVersion: ProtocolVersion,
  ): SubmissionRequest =
    create(
      sender,
      messageId,
      isRequest,
      batch,
      maxSequencingTime,
      timestampOfSigningKey,
      aggregationRule,
      protocolVersionRepresentativeFor(protocolVersion),
    ).valueOr(err => throw new IllegalArgumentException(err.message))

  def fromProtoV30(
      maxRequestSize: MaxRequestSizeToDeserialize,
      requestP: v30.SubmissionRequest,
  )(bytes: ByteString): ParsingResult[SubmissionRequest] = {
    val v30.SubmissionRequest(
      senderP,
      messageIdP,
      isRequest,
      batchP,
      maxSequencingTimeP,
      timestampOfSigningKey,
      aggregationRuleP,
    ) = requestP

    for {
      sender <- Member.fromProtoPrimitive(senderP, "sender")
      messageId <- MessageId.fromProtoPrimitive(messageIdP)
      maxSequencingTime <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "SubmissionRequest.maxSequencingTime",
        maxSequencingTimeP,
      )
      batch <- ProtoConverter.parseRequired(
        Batch.fromProtoV30(_, maxRequestSize),
        "SubmissionRequest.batch",
        batchP,
      )
      ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
      aggregationRule <- aggregationRuleP.traverse(AggregationRule.fromProtoV30)
    } yield new SubmissionRequest(
      sender,
      messageId,
      isRequest,
      batch,
      maxSequencingTime,
      ts,
      aggregationRule,
    )(protocolVersionRepresentativeFor(ProtoVersion(1)), Some(bytes))
  }
}
