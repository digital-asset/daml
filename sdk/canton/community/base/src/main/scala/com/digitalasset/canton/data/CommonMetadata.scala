// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
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
  *
  * @param salt
  *   Separate from UUID to be able to hide content of common metadata if it is blinded NOTE: we can
  *   get rid of this as we never blind the common metadata
  * @param uuid
  *   The mediator will deduplicate based on the UUID. The UUID is used to ensure ContractId
  *   uniqueness. The contract-ids are included in the computation of the update-id. So the chain is
  *   UUID -> Contract ID -> Update ID. If you would dedup on Update ID then you wouldn't guarantee
  *   unique contract IDs.
  */
final case class CommonMetadata private (
    synchronizerId: PhysicalSynchronizerId,
    mediator: MediatorGroupRecipient,
    salt: Salt,
    uuid: UUID,
)(
    hashOps: HashOps,
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[CommonMetadata](hashOps)
    with HasProtocolVersionedWrapper[CommonMetadata]
    with ProtocolVersionedMemoizedEvidence {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[CommonMetadata.type] =
    CommonMetadata.protocolVersionRepresentativeFor(synchronizerId.protocolVersion)

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
      physicalSynchronizerId = synchronizerId.toProtoPrimitive,
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
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.CommonMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def create(
      hashOps: HashOps
  )(
      synchronizerId: PhysicalSynchronizerId,
      mediator: MediatorGroupRecipient,
      salt: Salt,
      uuid: UUID,
  ): CommonMetadata =
    CommonMetadata(synchronizerId, mediator, salt, uuid)(
      hashOps,
      None,
    )

  private def fromProtoV30(hashOps: HashOps, metaDataP: v30.CommonMetadata)(
      bytes: ByteString
  ): ParsingResult[CommonMetadata] = {
    val v30.CommonMetadata(saltP, synchronizerIdP, uuidP, mediatorP) = metaDataP
    for {
      synchronizerId <- PhysicalSynchronizerId
        .fromProtoPrimitive(synchronizerIdP, "physical_synchronizer_id")

      mediatorGroup <- ProtoConverter.parseNonNegativeInt("mediator", mediatorP)
      mediatorGroupRecipient = MediatorGroupRecipient.apply(mediatorGroup)
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV30, "salt", saltP)
        .leftMap(_.inField("salt"))
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP).leftMap(_.inField("uuid"))
    } yield CommonMetadata(synchronizerId, mediatorGroupRecipient, salt, uuid)(
      hashOps,
      Some(bytes),
    )
  }
}
