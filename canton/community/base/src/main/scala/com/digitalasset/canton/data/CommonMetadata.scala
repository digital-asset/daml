// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CommonMetadata.singleMediatorError
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.{v0, *}
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
    mediator: MediatorRef,
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

  private def toProtoV0: v0.CommonMetadata = {
    mediator match {
      case MediatorRef.Single(mediatorId) =>
        v0.CommonMetadata(
          confirmationPolicy = confirmationPolicy.toProtoPrimitive,
          domainId = domainId.toProtoPrimitive,
          salt = Some(salt.toProtoV0),
          uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
          mediatorId = mediatorId.toProtoPrimitive,
        )
      case _ =>
        throw new IllegalStateException(singleMediatorError(representativeProtocolVersion))
    }
  }

  private def toProtoV1: v1.CommonMetadata = {
    v1.CommonMetadata(
      confirmationPolicy = confirmationPolicy.toProtoPrimitive,
      domainId = domainId.toProtoPrimitive,
      salt = Some(salt.toProtoV0),
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
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.CommonMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.CNTestNet)(v1.CommonMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  private def singleMediatorError(
      rpv: RepresentativeProtocolVersion[CommonMetadata.type]
  ): String = s"Only single mediator exist in for the representative protocol version $rpv"

  private[data] def shouldHaveSingleMediator(
      rpv: RepresentativeProtocolVersion[CommonMetadata.type]
  ): Boolean = rpv == protocolVersionRepresentativeFor(ProtocolVersion.v3)

  def create(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  )(
      confirmationPolicy: ConfirmationPolicy,
      domain: DomainId,
      mediator: MediatorRef,
      salt: Salt,
      uuid: UUID,
  ): Either[String, CommonMetadata] = create(
    hashOps,
    protocolVersionRepresentativeFor(protocolVersion),
  )(confirmationPolicy, domain, mediator, salt, uuid)

  def create(
      hashOps: HashOps,
      protocolVersion: RepresentativeProtocolVersion[CommonMetadata.type],
  )(
      confirmationPolicy: ConfirmationPolicy,
      domain: DomainId,
      mediator: MediatorRef,
      salt: Salt,
      uuid: UUID,
  ): Either[String, CommonMetadata] = {

    mediator match {
      case MediatorRef.Group(_) if shouldHaveSingleMediator(protocolVersion) =>
        Left(singleMediatorError(protocolVersion))

      case _ =>
        Right(
          CommonMetadata(confirmationPolicy, domain, mediator, salt, uuid)(
            hashOps,
            protocolVersion,
            None,
          )
        )
    }
  }

  private def fromProtoV0(hashOps: HashOps, metaDataP: v0.CommonMetadata)(
      bytes: ByteString
  ): ParsingResult[CommonMetadata] =
    for {
      confirmationPolicy <- ConfirmationPolicy
        .fromProtoPrimitive(metaDataP.confirmationPolicy)
        .leftMap(e =>
          ProtoDeserializationError.ValueDeserializationError("confirmationPolicy", e.show)
        )
      v0.CommonMetadata(saltP, _confirmationPolicyP, domainIdP, uuidP, mediatorIdP) = metaDataP
      domainUid <- UniqueIdentifier
        .fromProtoPrimitive_(domainIdP)
        .leftMap(ProtoDeserializationError.ValueDeserializationError("domainId", _))
      mediatorId <- MediatorId
        .fromProtoPrimitive(mediatorIdP, "CommonMetadata.mediator_id")
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", saltP)
        .leftMap(_.inField("salt"))
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP).leftMap(_.inField("uuid"))
    } yield CommonMetadata(
      confirmationPolicy,
      DomainId(domainUid),
      MediatorRef(mediatorId),
      salt,
      uuid,
    )(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )

  private def fromProtoV1(hashOps: HashOps, metaDataP: v1.CommonMetadata)(
      bytes: ByteString
  ): ParsingResult[CommonMetadata] =
    for {
      confirmationPolicy <- ConfirmationPolicy
        .fromProtoPrimitive(metaDataP.confirmationPolicy)
        .leftMap(e =>
          ProtoDeserializationError.ValueDeserializationError("confirmationPolicy", e.show)
        )
      v1.CommonMetadata(saltP, _confirmationPolicyP, domainIdP, uuidP, mediatorP) = metaDataP
      domainUid <- UniqueIdentifier
        .fromProtoPrimitive_(domainIdP)
        .leftMap(ProtoDeserializationError.ValueDeserializationError("domainId", _))
      mediator <- MediatorRef
        .fromProtoPrimitive(mediatorP, "CommonMetadata.mediator_id")
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", saltP)
        .leftMap(_.inField("salt"))
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP).leftMap(_.inField("uuid"))
    } yield CommonMetadata(confirmationPolicy, DomainId(domainUid), mediator, salt, uuid)(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(1)),
      Some(bytes),
    )
}
