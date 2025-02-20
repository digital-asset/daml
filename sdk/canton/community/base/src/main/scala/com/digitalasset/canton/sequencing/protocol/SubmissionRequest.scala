// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.{InvariantViolation, NonNegativeInt}
import com.digitalasset.canton.crypto.{HashOps, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  DeterministicEncoding,
  ProtoConverter,
  ProtocolVersionedMemoizedEvidence,
}
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.version.{
  DefaultValueUntilExclusive,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContextMemoizationWithDependency,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** @param topologyTimestamp
  *   The optional timestamp of the topology to use for processing this submission request. If
  *   [[scala.None$]], the snapshot at the sequencing time of the submission request should be used
  *   instead. The topology timestamp is used for:
  *   - resolving group addresses to members
  *   - checking signatures on closed envelopes
  *   - determining the signing key for the sequencer on the [[SequencedEvent]]s.
  *
  * @param aggregationRule
  *   If [[scala.Some$]], this submission request is aggregatable. Its envelopes will be delivered
  *   only when the rule's conditions are met. The receipt of delivery for an aggregatable
  *   submission will be delivered immediately to the sender even if the rule's conditions are not
  *   met.
  */
final case class SubmissionRequest private (
    sender: Member,
    messageId: MessageId,
    batch: Batch[ClosedEnvelope],
    maxSequencingTime: CantonTimestamp,
    topologyTimestamp: Option[CantonTimestamp],
    aggregationRule: Option[AggregationRule],
    submissionCost: Option[SequencingSubmissionCost],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SubmissionRequest.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends HasProtocolVersionedWrapper[SubmissionRequest]
    with ProtocolVersionedMemoizedEvidence {
  // Ensures the invariants related to default values hold
  validateInstance().valueOr(err => throw new IllegalArgumentException(err))

  @transient override protected lazy val companionObj: SubmissionRequest.type = SubmissionRequest

  def isConfirmationRequest: Boolean = {
    val hasParticipantRecipient = batch.allRecipients.exists {
      case MemberRecipient(_: ParticipantId) => true
      case _ => false
    }
    val hasMediatorRecipient = batch.allRecipients.exists {
      case _: MediatorGroupRecipient => true
      case _: Recipient => false
    }
    hasParticipantRecipient && hasMediatorRecipient
  }

  // Caches the serialized request to be able to do checks on its size without re-serializing
  lazy val toProtoV30: v30.SubmissionRequest = v30.SubmissionRequest(
    sender = sender.toProtoPrimitive,
    messageId = messageId.toProtoPrimitive,
    batch = Some(batch.toProtoV30),
    maxSequencingTime = maxSequencingTime.toProtoPrimitive,
    topologyTimestamp = topologyTimestamp.map(_.toProtoPrimitive),
    aggregationRule = aggregationRule.map(_.toProtoV30),
    submissionCost = submissionCost.map(_.toProtoV30),
  )

  def updateAggregationRule(aggregationRule: AggregationRule): SubmissionRequest =
    copy(aggregationRule = Some(aggregationRule))

  def updateMaxSequencingTime(maxSequencingTime: CantonTimestamp): SubmissionRequest =
    copy(maxSequencingTime = maxSequencingTime)

  @VisibleForTesting
  def copy(
      sender: Member = this.sender,
      messageId: MessageId = this.messageId,
      batch: Batch[ClosedEnvelope] = this.batch,
      maxSequencingTime: CantonTimestamp = this.maxSequencingTime,
      topologyTimestamp: Option[CantonTimestamp] = this.topologyTimestamp,
      aggregationRule: Option[AggregationRule] = this.aggregationRule,
      submissionCost: Option[SequencingSubmissionCost] = this.submissionCost,
  ) = SubmissionRequest
    .create(
      sender,
      messageId,
      batch,
      maxSequencingTime,
      topologyTimestamp,
      aggregationRule,
      submissionCost,
      representativeProtocolVersion,
    )
    .valueOr(err => throw new IllegalArgumentException(err.message))

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  /** Returns the [[AggregationId]] for grouping if this is an aggregatable submission. The
    * aggregation ID computationally authenticates the relevant contents of the submission request,
    * namely,
    *   - Envelope contents [[com.digitalasset.canton.sequencing.protocol.ClosedEnvelope.bytes]],
    *     the recipients [[com.digitalasset.canton.sequencing.protocol.ClosedEnvelope.recipients]]
    *     of the [[batch]], and whether there are signatures.
    *   - The [[maxSequencingTime]]
    *   - The [[topologyTimestamp]]
    *   - The [[aggregationRule]]
    *
    * The [[AggregationId]] does not authenticate the following pieces of a submission request:
    *   - The signatures [[com.digitalasset.canton.sequencing.protocol.ClosedEnvelope.signatures]]
    *     on the closed envelopes because the signatures differ for each sender. Aggregating the
    *     signatures is the whole point of an aggregatable submission. In contrast, the presence of
    *     signatures is relevant for the ID because it determines how the
    *     [[com.digitalasset.canton.sequencing.protocol.ClosedEnvelope.bytes]] are interpreted.
    *   - The [[sender]] and the [[messageId]], as they are specific to the sender of a particular
    *     submission request
    *   - The [[isConfirmationRequest]] flag because it is irrelevant for delivery or aggregation
    */
  def aggregationId(hashOps: HashOps): Either[ProtoDeserializationError, Option[AggregationId]] = {
    // TODO(#12075) Use a deterministic serialization scheme for the recipients
    val recipientsSerializerE: Either[ProtoDeserializationError, Recipients => ByteString] =
      SubmissionRequest.converterFor(representativeProtocolVersion).map(_.dependencySerializer)

    aggregationRule.traverse { rule =>
      recipientsSerializerE.map { recipientsSerializer =>
        aggregationIdInternal(hashOps, rule, recipientsSerializer)
      }
    }
  }

  private def aggregationIdInternal(
      hashOps: HashOps,
      rule: AggregationRule,
      recipientsSerializer: Recipients => ByteString,
  ): AggregationId = {
    val builder = hashOps.build(HashPurpose.AggregationId)
    builder.add(batch.envelopes.length)
    batch.envelopes.foreach { envelope =>
      val ClosedEnvelope(content, recipients, signatures) = envelope
      builder.add(DeterministicEncoding.encodeBytes(content))
      builder.add(
        DeterministicEncoding.encodeBytes(
          recipientsSerializer(recipients)
        )
      )
      builder.add(DeterministicEncoding.encodeByte(if (signatures.isEmpty) 0x00 else 0x01))
    }
    builder.add(maxSequencingTime.underlying.micros)
    // CantonTimestamp's microseconds can never be Long.MinValue, so the encoding remains injective if we use Long.MaxValue as the default.
    builder.add(topologyTimestamp.fold(Long.MinValue)(_.underlying.micros))
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
    extends VersioningCompanionContextMemoizationWithDependency[
      SubmissionRequest,
      MaxRequestSizeToDeserialize,
      // Recipients is a dependency because its versioning scheme needs to be aligned with this one
      // such that SubmissionRequest and Recipiients can be versioned independently
      Recipients,
    ] {

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec.withDependency(
      ProtocolVersion.v33
    )(v30.SubmissionRequest)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30, // Serialization of SubmissionRequest
      _.toProtoV30, // Serialization of Recipients
    )
  )

  lazy val submissionCostDefaultValue
      : DefaultValueUntilExclusive[SubmissionRequest, this.type, Option[SequencingSubmissionCost]] =
    DefaultValueUntilExclusive(
      _.submissionCost,
      "submissionCost",
      protocolVersionRepresentativeFor(ProtocolVersion.dev),
      None,
    )

  override def name: String = "submission request"

  // TODO(i17584): revisit the consequences of no longer enforcing that
  //  aggregated submissions with signed envelopes define a topology snapshot
  override lazy val invariants: Invariants = Seq.empty

  def create(
      sender: Member,
      messageId: MessageId,
      batch: Batch[ClosedEnvelope],
      maxSequencingTime: CantonTimestamp,
      topologyTimestamp: Option[CantonTimestamp],
      aggregationRule: Option[AggregationRule],
      submissionCost: Option[SequencingSubmissionCost],
      representativeProtocolVersion: RepresentativeProtocolVersion[SubmissionRequest.type],
  ): Either[InvariantViolation, SubmissionRequest] =
    Either
      .catchOnly[IllegalArgumentException](
        new SubmissionRequest(
          sender,
          messageId,
          batch,
          maxSequencingTime,
          topologyTimestamp,
          aggregationRule,
          submissionCost,
        )(representativeProtocolVersion, deserializedFrom = None)
      )
      .leftMap(error => InvariantViolation(error.getMessage))

  def tryCreate(
      sender: Member,
      messageId: MessageId,
      batch: Batch[ClosedEnvelope],
      maxSequencingTime: CantonTimestamp,
      topologyTimestamp: Option[CantonTimestamp],
      aggregationRule: Option[AggregationRule],
      submissionCost: Option[SequencingSubmissionCost],
      protocolVersion: ProtocolVersion,
  ): SubmissionRequest =
    create(
      sender,
      messageId,
      batch,
      maxSequencingTime,
      topologyTimestamp,
      aggregationRule,
      submissionCost,
      protocolVersionRepresentativeFor(protocolVersion),
    ).valueOr(err => throw new IllegalArgumentException(err.message))

  def fromProtoV30(
      maxRequestSize: MaxRequestSizeToDeserialize,
      requestP: v30.SubmissionRequest,
  )(bytes: ByteString): ParsingResult[SubmissionRequest] = {
    val v30.SubmissionRequest(
      senderP,
      messageIdP,
      batchP,
      maxSequencingTimeP,
      topologyTimestamp,
      aggregationRuleP,
      submissionCostP,
    ) = requestP

    for {
      sender <- Member.fromProtoPrimitive(senderP, "sender")
      messageId <- MessageId.fromProtoPrimitive(messageIdP)
      maxSequencingTime <- CantonTimestamp.fromProtoPrimitive(maxSequencingTimeP)
      batch <- ProtoConverter.parseRequired(
        Batch.fromProtoV30(_, maxRequestSize),
        "SubmissionRequest.batch",
        batchP,
      )
      ts <- topologyTimestamp.traverse(CantonTimestamp.fromProtoPrimitive)
      aggregationRule <- aggregationRuleP.traverse(AggregationRule.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      submissionCost <- submissionCostP.traverse(SequencingSubmissionCost.fromProtoV30)
    } yield new SubmissionRequest(
      sender,
      messageId,
      batch,
      maxSequencingTime,
      ts,
      aggregationRule,
      submissionCost,
    )(rpv, Some(bytes))
  }
}
