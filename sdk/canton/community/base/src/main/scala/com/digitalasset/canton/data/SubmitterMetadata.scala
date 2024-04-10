// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.lf.data.Ref
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Information about the submitters of the transaction
  * `maxSequencingTimeO` was added in PV=5, so it will only be defined for PV >= 5, and will be `None` otherwise.
  */
final case class SubmitterMetadata private (
    actAs: NonEmpty[Set[LfPartyId]],
    applicationId: ApplicationId,
    commandId: CommandId,
    submittingParticipant: ParticipantId,
    salt: Salt,
    submissionId: Option[LedgerSubmissionId],
    dedupPeriod: DeduplicationPeriod,
    maxSequencingTime: CantonTimestamp,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SubmitterMetadata.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[SubmitterMetadata](hashOps)
    with HasProtocolVersionedWrapper[SubmitterMetadata]
    with ProtocolVersionedMemoizedEvidence {

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override val hashPurpose: HashPurpose = HashPurpose.SubmitterMetadata

  override def pretty: Pretty[SubmitterMetadata] = prettyOfClass(
    param("act as", _.actAs),
    param("application id", _.applicationId),
    param("command id", _.commandId),
    param("submitting participant", _.submittingParticipant),
    param("salt", _.salt),
    paramIfDefined("submission id", _.submissionId),
    param("deduplication period", _.dedupPeriod),
    param("max sequencing time", _.maxSequencingTime),
  )

  @transient override protected lazy val companionObj: SubmitterMetadata.type = SubmitterMetadata

  protected def toProtoV30: v30.SubmitterMetadata = v30.SubmitterMetadata(
    actAs = actAs.toSeq,
    applicationId = applicationId.toProtoPrimitive,
    commandId = commandId.toProtoPrimitive,
    submittingParticipant = submittingParticipant.toProtoPrimitive,
    salt = Some(salt.toProtoV30),
    submissionId = submissionId.getOrElse(""),
    dedupPeriod = Some(SerializableDeduplicationPeriod(dedupPeriod).toProtoV30),
    maxSequencingTime = maxSequencingTime.toProtoPrimitive,
  )
}

object SubmitterMetadata
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      SubmitterMetadata,
      HashOps,
    ] {
  override val name: String = "SubmitterMetadata"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.SubmitterMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def apply(
      actAs: NonEmpty[Set[LfPartyId]],
      applicationId: ApplicationId,
      commandId: CommandId,
      submittingParticipant: ParticipantId,
      salt: Salt,
      submissionId: Option[LedgerSubmissionId],
      dedupPeriod: DeduplicationPeriod,
      maxSequencingTime: CantonTimestamp,
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): SubmitterMetadata = SubmitterMetadata(
    actAs, // Canton ignores SubmitterInfo.readAs per https://github.com/digital-asset/daml/pull/12136
    applicationId,
    commandId,
    submittingParticipant,
    salt,
    submissionId,
    dedupPeriod,
    maxSequencingTime,
  )(hashOps, protocolVersionRepresentativeFor(protocolVersion), None)

  def fromSubmitterInfo(hashOps: HashOps)(
      submitterActAs: List[Ref.Party],
      submitterApplicationId: Ref.ApplicationId,
      submitterCommandId: Ref.CommandId,
      submitterSubmissionId: Option[Ref.SubmissionId],
      submitterDeduplicationPeriod: DeduplicationPeriod,
      submittingParticipant: ParticipantId,
      salt: Salt,
      maxSequencingTime: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  ): Either[String, SubmitterMetadata] = {
    NonEmpty.from(submitterActAs.toSet).toRight("The actAs set must not be empty.").map {
      actAsNes =>
        SubmitterMetadata(
          actAsNes, // Canton ignores SubmitterInfo.readAs per https://github.com/digital-asset/daml/pull/12136
          ApplicationId(submitterApplicationId),
          CommandId(submitterCommandId),
          submittingParticipant,
          salt,
          submitterSubmissionId,
          submitterDeduplicationPeriod,
          maxSequencingTime,
          hashOps,
          protocolVersion,
        )
    }
  }

  private def fromProtoV30(hashOps: HashOps, metaDataP: v30.SubmitterMetadata)(
      bytes: ByteString
  ): ParsingResult[SubmitterMetadata] = {
    val v30.SubmitterMetadata(
      saltOP,
      actAsP,
      applicationIdP,
      commandIdP,
      submittingParticipantP,
      submissionIdP,
      dedupPeriodOP,
      maxSequencingTimeOP,
    ) = metaDataP

    for {
      submittingParticipant <- ParticipantId
        .fromProtoPrimitive(submittingParticipantP, "SubmitterMetadata.submitter_participant")
      actAs <- actAsP.traverse(
        ProtoConverter
          .parseLfPartyId(_)
          .leftMap(e => ProtoDeserializationError.ValueConversionError("actAs", e.message))
      )
      applicationId <- ApplicationId
        .fromProtoPrimitive(applicationIdP)
        .leftMap(ProtoDeserializationError.ValueConversionError("applicationId", _))
      commandId <- CommandId
        .fromProtoPrimitive(commandIdP)
        .leftMap(ProtoDeserializationError.ValueConversionError("commandId", _))
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV30, "salt", saltOP)
        .leftMap(e => ProtoDeserializationError.ValueConversionError("salt", e.message))
      submissionIdO <- Option
        .when(submissionIdP.nonEmpty)(submissionIdP)
        .traverse(
          LedgerSubmissionId
            .fromString(_)
            .leftMap(ProtoDeserializationError.ValueConversionError("submissionId", _))
        )
      dedupPeriod <- ProtoConverter
        .parseRequired(
          SerializableDeduplicationPeriod.fromProtoV30,
          "SubmitterMetadata.deduplication_period",
          dedupPeriodOP,
        )
        .leftMap(e =>
          ProtoDeserializationError.ValueConversionError("deduplicationPeriod", e.message)
        )
      actAsNes <- NonEmpty
        .from(actAs.toSet)
        .toRight(
          ProtoDeserializationError.ValueConversionError("acsAs", "actAs set must not be empty.")
        )
      maxSequencingTime <- CantonTimestamp.fromProtoPrimitive(maxSequencingTimeOP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SubmitterMetadata(
      actAsNes,
      applicationId,
      commandId,
      submittingParticipant,
      salt,
      submissionIdO,
      dedupPeriod,
      maxSequencingTime,
    )(hashOps, rpv, Some(bytes))
  }
}
