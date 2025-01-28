// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import java.util.UUID

/** Information concerning every '''member''' involved in the underlying transaction.
  */
final case class CommonMetadata private (
    synchronizerId: SynchronizerId,
    mediator: MediatorGroupRecipient,
    salt: Salt,
    uuid: UUID,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[CommonMetadata.type],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[CommonMetadata](hashOps)
    with HasProtocolVersionedWrapper[CommonMetadata]
    with ProtocolVersionedMemoizedEvidence {

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override val hashPurpose: HashPurpose = HashPurpose.CommonMetadata

  override protected def pretty: Pretty[CommonMetadata] = prettyOfClass(
    param("synchronizer id", _.synchronizerId),
    param("mediator", _.mediator),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )

  @transient override protected lazy val companionObj: CommonMetadata.type = CommonMetadata

  private def toProtoV30: v30.CommonMetadata =
    v30.CommonMetadata(
      synchronizerId = synchronizerId.toProtoPrimitive,
      salt = Some(salt.toProtoV30),
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      mediatorGroup = mediator.group.value,
    )
}

object CommonMetadata
    extends VersioningCompanionContextMemoization[
      CommonMetadata,
      HashOps,
    ] {
  override val name: String = "CommonMetadata"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.CommonMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def create(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  )(
      synchronizerId: SynchronizerId,
      mediator: MediatorGroupRecipient,
      salt: Salt,
      uuid: UUID,
  ): CommonMetadata = create(
    hashOps,
    protocolVersionRepresentativeFor(protocolVersion),
  )(synchronizerId, mediator, salt, uuid)

  def create(
      hashOps: HashOps,
      protocolVersion: RepresentativeProtocolVersion[CommonMetadata.type],
  )(
      synchronizerId: SynchronizerId,
      mediator: MediatorGroupRecipient,
      salt: Salt,
      uuid: UUID,
  ): CommonMetadata =
    CommonMetadata(synchronizerId, mediator, salt, uuid)(
      hashOps,
      protocolVersion,
      None,
    )

  private def fromProtoV30(hashOps: HashOps, metaDataP: v30.CommonMetadata)(
      bytes: ByteString
  ): ParsingResult[CommonMetadata] = {
    val v30.CommonMetadata(saltP, synchronizerIdP, uuidP, mediatorP) = metaDataP
    for {
      synchronizerUid <- UniqueIdentifier
        .fromProtoPrimitive_(synchronizerIdP)
        .leftMap(e =>
          ProtoDeserializationError.ValueDeserializationError("synchronizer_id", e.message)
        )
      mediatorGroup <- ProtoConverter.parseNonNegativeInt("mediator", mediatorP)
      mediatorGroupRecipient = MediatorGroupRecipient.apply(mediatorGroup)
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV30, "salt", saltP)
        .leftMap(_.inField("salt"))
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP).leftMap(_.inField("uuid"))
      pv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield CommonMetadata(SynchronizerId(synchronizerUid), mediatorGroupRecipient, salt, uuid)(
      hashOps,
      pv,
      Some(bytes),
    )
  }
}
