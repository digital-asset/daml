// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    userId: UserId,
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
    param("user id", _.userId),
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
    userId = userId.toProtoPrimitive,
    commandId = commandId.toProtoPrimitive,
    submittingParticipantUid = submittingParticipant.uid.toProtoPrimitive,
    salt = Some(salt.toProtoV30),
    submissionId = submissionId.getOrElse(""),
    dedupPeriod = Some(SerializableDeduplicationPeriod(dedupPeriod).toProtoV30),
    maxSequencingTime = maxSequencingTime.toProtoPrimitive,
    externalAuthorization = externalAuthorization.map(_.toProtoV30),
  )

  protected def toProtoV31: v31.SubmitterMetadata = v31.SubmitterMetadata(
    actAs = actAs.toSeq,
    userId = userId.toProtoPrimitive,
    commandId = commandId.toProtoPrimitive,
    submittingParticipantUid = submittingParticipant.uid.toProtoPrimitive,
    salt = Some(salt.toProtoV30),
    submissionId = submissionId.getOrElse(""),
    dedupPeriod = Some(SerializableDeduplicationPeriod(dedupPeriod).toProtoV30),
    maxSequencingTime = maxSequencingTime.toProtoPrimitive,
    externalAuthorization = externalAuthorization.map(_.toProtoV31),
  )

}

object SubmitterMetadata
    extends VersioningCompanionContextMemoization[
      SubmitterMetadata,
      HashOps,
    ] {
  override val name: String = "SubmitterMetadata"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.SubmitterMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    ),
    ProtoVersion(31) -> VersionedProtoCodec(ProtocolVersion.v35)(v31.SubmitterMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV31),
      _.toProtoV31,
    ),
  )

  def apply(
      actAs: NonEmpty[Set[LfPartyId]],
      userId: UserId,
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
    userId,
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
      submitterUserId: Ref.UserId,
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
          UserId(submitterUserId),
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
      userIdP,
      commandIdP,
      submittingParticipantUidP,
      submissionIdP,
      dedupPeriodOP,
      maxSequencingTimeOP,
      externalAuthorizationOP,
    ) = metaDataP

    for {
      externalAuthorizationO <- externalAuthorizationOP.traverse(
        ExternalAuthorization.fromProtoV30
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      result <- fromProto(hashOps, bytes)(
        saltOP,
        actAsP,
        userIdP,
        commandIdP,
        submittingParticipantUidP,
        submissionIdP,
        dedupPeriodOP,
        maxSequencingTimeOP,
        externalAuthorizationO,
        rpv,
      )

    } yield result

  }

  private def fromProtoV31(hashOps: HashOps, metaDataP: v31.SubmitterMetadata)(
      bytes: ByteString
  ): ParsingResult[SubmitterMetadata] = {
    val v31.SubmitterMetadata(
      saltOP,
      actAsP,
      userIdP,
      commandIdP,
      submittingParticipantUidP,
      submissionIdP,
      dedupPeriodOP,
      maxSequencingTimeOP,
      externalAuthorizationOP,
    ) = metaDataP

    for {
      externalAuthorizationO <- externalAuthorizationOP.traverse(
        ExternalAuthorization.fromProtoV31
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))
      result <- fromProto(hashOps, bytes)(
        saltOP,
        actAsP,
        userIdP,
        commandIdP,
        submittingParticipantUidP,
        submissionIdP,
        dedupPeriodOP,
        maxSequencingTimeOP,
        externalAuthorizationO,
        rpv,
      )
    } yield result
  }

  private def fromProto(hashOps: HashOps, bytes: DataByteString)(
      saltOP: Option[com.digitalasset.canton.crypto.v30.Salt],
      actAsP: Seq[String],
      userIdP: String,
      commandIdP: String,
      submittingParticipantUidP: String,
      submissionIdP: String,
      dedupPeriodOP: Option[v30.DeduplicationPeriod],
      maxSequencingTimeOP: Long,
      externalAuthorizationO: Option[ExternalAuthorization],
      rpv: RepresentativeProtocolVersion[SubmitterMetadata.type],
  ): ParsingResult[SubmitterMetadata] =
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
      userId <- UserId
        .fromProtoPrimitive(userIdP)
        .leftMap(ProtoDeserializationError.ValueConversionError("userId", _))
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
    } yield SubmitterMetadata(
      actAsNes,
      userId,
      commandId,
      submittingParticipant,
      salt,
      submissionIdO,
      dedupPeriod,
      maxSequencingTime,
      externalAuthorizationO,
    )(hashOps, rpv, Some(bytes))

}
