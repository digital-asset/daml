// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.{ConfirmationPolicy, v30}
import com.digitalasset.canton.sequencing.protocol.MediatorsOfDomain
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import java.util.UUID

/** Information concerning every '''member''' involved in the underlying transaction.
  *
  * @param confirmationPolicy determines who must confirm the request
  */
final case class CommonMetadata private (
    confirmationPolicy: ConfirmationPolicy,
    domainId: DomainId,
    mediator: MediatorsOfDomain,
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

  override def pretty: Pretty[CommonMetadata] = prettyOfClass(
    param("confirmation policy", _.confirmationPolicy),
    param("domain id", _.domainId),
    param("mediator", _.mediator),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )

  @transient override protected lazy val companionObj: CommonMetadata.type = CommonMetadata

  private def toProtoV30: v30.CommonMetadata = {
    v30.CommonMetadata(
      confirmationPolicy = confirmationPolicy.toProtoPrimitive,
      domainId = domainId.toProtoPrimitive,
      salt = Some(salt.toProtoV30),
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      mediator = mediator.toProtoPrimitive,
    )
  }
}

object CommonMetadata
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      CommonMetadata,
      HashOps,
    ] {
  override val name: String = "CommonMetadata"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.CommonMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  )(
      confirmationPolicy: ConfirmationPolicy,
      domain: DomainId,
      mediator: MediatorsOfDomain,
      salt: Salt,
      uuid: UUID,
  ): CommonMetadata = create(
    hashOps,
    protocolVersionRepresentativeFor(protocolVersion),
  )(confirmationPolicy, domain, mediator, salt, uuid)

  def create(
      hashOps: HashOps,
      protocolVersion: RepresentativeProtocolVersion[CommonMetadata.type],
  )(
      confirmationPolicy: ConfirmationPolicy,
      domain: DomainId,
      mediator: MediatorsOfDomain,
      salt: Salt,
      uuid: UUID,
  ): CommonMetadata =
    CommonMetadata(confirmationPolicy, domain, mediator, salt, uuid)(
      hashOps,
      protocolVersion,
      None,
    )

  private def fromProtoV30(hashOps: HashOps, metaDataP: v30.CommonMetadata)(
      bytes: ByteString
  ): ParsingResult[CommonMetadata] =
    for {
      confirmationPolicy <- ConfirmationPolicy
        .fromProtoPrimitive(metaDataP.confirmationPolicy)
        .leftMap(e =>
          ProtoDeserializationError.ValueDeserializationError("confirmationPolicy", e.show)
        )
      v30.CommonMetadata(saltP, _confirmationPolicyP, domainIdP, uuidP, mediatorP) = metaDataP
      domainUid <- UniqueIdentifier
        .fromProtoPrimitive_(domainIdP)
        .leftMap(ProtoDeserializationError.ValueDeserializationError("domainId", _))
      mediator <- MediatorsOfDomain
        .fromProtoPrimitive(mediatorP, "CommonMetadata.mediator")
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV30, "salt", saltP)
        .leftMap(_.inField("salt"))
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP).leftMap(_.inField("uuid"))
      pv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield CommonMetadata(confirmationPolicy, DomainId(domainUid), mediator, salt, uuid)(
      hashOps,
      pv,
      Some(bytes),
    )
}
