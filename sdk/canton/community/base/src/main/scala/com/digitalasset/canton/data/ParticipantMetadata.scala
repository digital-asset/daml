// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Information concerning every '''participant''' involved in the underlying transaction.
  *
  * @param ledgerTime     The ledger time of the transaction
  * @param submissionTime The submission time of the transaction
  * @param workflowIdO    optional workflow id associated with the ledger api provided workflow instance
  */
final case class ParticipantMetadata private (
    ledgerTime: CantonTimestamp,
    submissionTime: CantonTimestamp,
    workflowIdO: Option[WorkflowId],
    salt: Salt,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ParticipantMetadata.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[ParticipantMetadata](hashOps)
    with HasProtocolVersionedWrapper[ParticipantMetadata]
    with ProtocolVersionedMemoizedEvidence {

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override val hashPurpose: HashPurpose = HashPurpose.ParticipantMetadata

  override protected def pretty: Pretty[ParticipantMetadata] = prettyOfClass(
    param("ledger time", _.ledgerTime),
    param("submission time", _.submissionTime),
    paramIfDefined("workflow id", _.workflowIdO),
    param("salt", _.salt),
  )

  @transient override protected lazy val companionObj: ParticipantMetadata.type =
    ParticipantMetadata

  private def toProtoV30: v30.ParticipantMetadata = v30.ParticipantMetadata(
    ledgerTime = ledgerTime.toProtoPrimitive,
    submissionTime = submissionTime.toProtoPrimitive,
    workflowId = workflowIdO.fold("")(_.toProtoPrimitive),
    salt = Some(salt.toProtoV30),
  )
}

object ParticipantMetadata
    extends VersioningCompanionContextMemoization[ParticipantMetadata, HashOps] {
  override val name: String = "ParticipantMetadata"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.ParticipantMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def apply(hashOps: HashOps)(
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      workflowId: Option[WorkflowId],
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): ParticipantMetadata =
    ParticipantMetadata(ledgerTime, submissionTime, workflowId, salt)(
      hashOps,
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV30(hashOps: HashOps, metadataP: v30.ParticipantMetadata)(
      bytes: ByteString
  ): ParsingResult[ParticipantMetadata] = {
    val v30.ParticipantMetadata(saltP, ledgerTimeP, submissionTimeP, workflowIdP) = metadataP
    for {
      let <- CantonTimestamp.fromProtoPrimitive(ledgerTimeP)
      submissionTime <- CantonTimestamp.fromProtoPrimitive(submissionTimeP)
      workflowId <- workflowIdP match {
        case "" => Right(None)
        case wf =>
          WorkflowId
            .fromProtoPrimitive(wf)
            .map(Some(_))
            .leftMap(ProtoDeserializationError.ValueDeserializationError("workflowId", _))
      }
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV30, "salt", saltP)
        .leftMap(_.inField("salt"))
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield ParticipantMetadata(let, submissionTime, workflowId, salt)(
      hashOps,
      rpv,
      Some(bytes),
    )
  }
}
