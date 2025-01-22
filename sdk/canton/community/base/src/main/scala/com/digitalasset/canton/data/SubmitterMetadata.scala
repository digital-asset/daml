// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.*
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString

/** Information about the submitters of the transaction */
final case class SubmitterMetadata private (
    actAs: NonEmpty[Set[LfPartyId]],
    applicationId: ApplicationId,
    commandId: CommandId,
    submittingParticipant: ParticipantId,
    salt: Salt,
    submissionId: Option[LedgerSubmissionId],
    dedupPeriod: DeduplicationPeriod,
    maxSequencingTime: CantonTimestamp,
    externalAuthorization: Option[ExternalAuthorization],
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SubmitterMetadata.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[SubmitterMetadata](hashOps)
    with HasProtocolVersionedWrapper[SubmitterMetadata]
    with ProtocolVersionedMemoizedEvidence
    with HasSubmissionTrackerData {

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override val hashPurpose: HashPurpose = HashPurpose.SubmitterMetadata

  override def submissionTrackerData: Option[SubmissionTrackerData] = Some(
    SubmissionTrackerData(submittingParticipant, maxSequencingTime)
  )

  override protected def pretty: Pretty[SubmitterMetadata] = prettyOfClass(
    param("act as", _.actAs),
    param("application id", _.applicationId),
    param("command id", _.commandId),
    param("submitting participant", _.submittingParticipant),
    param("salt", _.salt),
    paramIfDefined("submission id", _.submissionId),
    param("deduplication period", _.dedupPeriod),
    param("max sequencing time", _.maxSequencingTime),
    paramIfDefined("external authorization", _.externalAuthorization),
  )

  @transient override protected lazy val companionObj: SubmitterMetadata.type = SubmitterMetadata

  protected def toProtoV30: v30.SubmitterMetadata = v30.SubmitterMetadata(
    actAs = actAs.toSeq,
    applicationId = applicationId.toProtoPrimitive,
    commandId = commandId.toProtoPrimitive,
    submittingParticipantUid = submittingParticipant.uid.toProtoPrimitive,
    salt = Some(salt.toProtoV30),
    submissionId = submissionId.getOrElse(""),
    dedupPeriod = Some(SerializableDeduplicationPeriod(dedupPeriod).toProtoV30),
    maxSequencingTime = maxSequencingTime.toProtoPrimitive,
    externalAuthorization = externalAuthorization.map(_.toProtoV30),
  )
}

object SubmitterMetadata
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      SubmitterMetadata,
      HashOps,
    ] {
  override val name: String = "SubmitterMetadata"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v33)(v30.SubmitterMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
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
      externalAuthorization: Option[ExternalAuthorization],
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
    externalAuthorization,
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
      externalAuthorization: Option[ExternalAuthorization],
      protocolVersion: ProtocolVersion,
  ): Either[String, SubmitterMetadata] =
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
          externalAuthorization,
          hashOps,
          protocolVersion,
        )
    }

  private def fromProtoV30(hashOps: HashOps, metaDataP: v30.SubmitterMetadata)(
      bytes: ByteString
  ): ParsingResult[SubmitterMetadata] = {
    val v30.SubmitterMetadata(
      saltOP,
      actAsP,
      applicationIdP,
      commandIdP,
      submittingParticipantUidP,
      submissionIdP,
      dedupPeriodOP,
      maxSequencingTimeOP,
      externalAuthorizationOP,
    ) = metaDataP

    for {
      submittingParticipant <- UniqueIdentifier
        .fromProtoPrimitive(
          submittingParticipantUidP,
          "SubmitterMetadata.submitter_participant_uid",
        )
        .map(ParticipantId(_))
      actAs <- actAsP.traverse(
        ProtoConverter
          .parseLfPartyId(_, "act_as")
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
      externalAuthorizationO <- externalAuthorizationOP.traverse(
        ExternalAuthorization.fromProtoV30
      )
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
      externalAuthorizationO,
    )(hashOps, rpv, Some(bytes))
  }
}
