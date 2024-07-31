// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v0, v1, v2, v3}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

sealed trait EnvelopeContent
    extends HasProtocolVersionedWrapper[EnvelopeContent]
    with Product
    with Serializable {
  def message: ProtocolMessage

  @transient override protected lazy val companionObj: EnvelopeContent.type = EnvelopeContent

  def toByteStringUnversioned: ByteString
}

sealed abstract case class EnvelopeContentV0(override val message: ProtocolMessageV0)
    extends EnvelopeContent {
  override def toByteStringUnversioned: ByteString = message.toProtoEnvelopeContentV0.toByteString

  override def representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent.type] =
    EnvelopeContent.representativeV0
}

sealed abstract case class EnvelopeContentV1(override val message: ProtocolMessageV1)
    extends EnvelopeContent {
  override def toByteStringUnversioned: ByteString = message.toProtoEnvelopeContentV1.toByteString

  override def representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent.type] =
    EnvelopeContent.representativeV1
}

sealed abstract case class EnvelopeContentV2(override val message: ProtocolMessageV2)
    extends EnvelopeContent {
  override def toByteStringUnversioned: ByteString = message.toProtoEnvelopeContentV2.toByteString

  override def representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent.type] =
    EnvelopeContent.representativeV2
}

sealed abstract case class EnvelopeContentV3(override val message: ProtocolMessageV3)
    extends EnvelopeContent {
  override def toByteStringUnversioned: ByteString = message.toProtoEnvelopeContentV3.toByteString

  override def representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent.type] =
    EnvelopeContent.representativeV3
}

object EnvelopeContent
    extends HasProtocolVersionedWithContextCompanion[EnvelopeContent, (HashOps, ProtocolVersion)] {

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> LegacyProtoConverter(ProtocolVersion.v3)(v0.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toByteStringUnversioned,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV1),
      _.toByteStringUnversioned,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v5)(v2.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV2),
      _.toByteStringUnversioned,
    ),
    ProtoVersion(3) -> VersionedProtoConverter(ProtocolVersion.v6)(v3.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV3),
      _.toByteStringUnversioned,
    ),
  )

  private[messages] val representativeV0: RepresentativeProtocolVersion[EnvelopeContent.type] =
    tryProtocolVersionRepresentativeFor(ProtoVersion(0))
  private[messages] val representativeV1: RepresentativeProtocolVersion[EnvelopeContent.type] =
    tryProtocolVersionRepresentativeFor(ProtoVersion(1))
  private[messages] val representativeV2: RepresentativeProtocolVersion[EnvelopeContent.type] =
    tryProtocolVersionRepresentativeFor(ProtoVersion(2))
  private[messages] val representativeV3: RepresentativeProtocolVersion[EnvelopeContent.type] =
    tryProtocolVersionRepresentativeFor(ProtoVersion(3))

  def create(
      message: ProtocolMessage,
      protocolVersion: ProtocolVersion,
  ): Either[String, EnvelopeContent] = {
    val representativeProtocolVersion = protocolVersionRepresentativeFor(protocolVersion)
    message match {
      case messageV3: ProtocolMessageV3
          if representativeProtocolVersion == EnvelopeContent.representativeV3 =>
        Right(new EnvelopeContentV3(messageV3) {})
      case messageV2: ProtocolMessageV2
          if representativeProtocolVersion == EnvelopeContent.representativeV2 =>
        Right(new EnvelopeContentV2(messageV2) {})
      case messageV1: ProtocolMessageV1
          if representativeProtocolVersion == EnvelopeContent.representativeV1 =>
        Right(new EnvelopeContentV1(messageV1) {})
      case messageV0: ProtocolMessageV0
          if representativeProtocolVersion == EnvelopeContent.representativeV0 =>
        Right(new EnvelopeContentV0(messageV0) {})
      case _ =>
        Left(s"Cannot use message $message in protocol version $protocolVersion")
    }
  }

  def tryCreate(
      message: ProtocolMessage,
      protocolVersion: ProtocolVersion,
  ): EnvelopeContent =
    create(message, protocolVersion).valueOr(err => throw new IllegalArgumentException(err))

  private def fromProtoV0(
      context: (HashOps, ProtocolVersion),
      envelopeContent: v0.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    val (hashOps, expectedProtocolVersion) = context
    import v0.EnvelopeContent.SomeEnvelopeContent as Content
    val messageE = (envelopeContent.someEnvelopeContent match {
      case Content.InformeeMessage(messageP) =>
        InformeeMessage.fromProtoV0(context)(messageP)
      case Content.DomainTopologyTransactionMessage(messageP) =>
        DomainTopologyTransactionMessage.fromProtoV0(expectedProtocolVersion, messageP)
      case Content.EncryptedViewMessage(messageP) =>
        EncryptedViewMessageV0.fromProto(messageP)
      case Content.SignedMessage(messageP) =>
        SignedProtocolMessage.fromProtoV0(context, messageP)
      case Content.TransferOutMediatorMessage(messageP) =>
        TransferOutMediatorMessage.fromProtoV0(
          (hashOps, SourceProtocolVersion(expectedProtocolVersion))
        )(messageP)
      case Content.TransferInMediatorMessage(messageP) =>
        TransferInMediatorMessage.fromProtoV0(
          (hashOps, TargetProtocolVersion(expectedProtocolVersion))
        )(messageP)
      case Content.RootHashMessage(messageP) =>
        RootHashMessage.fromProtoV0(SerializedRootHashMessagePayload.fromByteString)(messageP)
      case Content.RegisterTopologyTransactionRequest(messageP) =>
        RegisterTopologyTransactionRequest.fromProtoV0(expectedProtocolVersion, messageP)
      case Content.RegisterTopologyTransactionResponse(messageP) =>
        RegisterTopologyTransactionResponse.fromProtoV0(messageP)
      case Content.CausalityMessage(messageP) => CausalityMessage.fromProtoV0(messageP)
      case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
    }): ParsingResult[ProtocolMessageV0]
    messageE.map(message => new EnvelopeContentV0(message) {})
  }

  private def fromProtoV1(
      context: (HashOps, ProtocolVersion),
      envelopeContent: v1.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    val (hashOps, expectedProtocolVersion) = context
    import v1.EnvelopeContent.SomeEnvelopeContent as Content
    val messageE = (envelopeContent.someEnvelopeContent match {
      case Content.InformeeMessage(messageP) =>
        InformeeMessage.fromProtoV1(context)(messageP)
      case Content.DomainTopologyTransactionMessage(messageP) =>
        DomainTopologyTransactionMessage.fromProtoV0(expectedProtocolVersion, messageP)
      case Content.EncryptedViewMessage(messageP) =>
        EncryptedViewMessageV1.fromProto(messageP)
      case Content.SignedMessage(messageP) =>
        SignedProtocolMessage.fromProtoV0(context, messageP)
      case Content.TransferOutMediatorMessage(messageP) =>
        TransferOutMediatorMessage.fromProtoV1(
          (hashOps, SourceProtocolVersion(expectedProtocolVersion))
        )(messageP)
      case Content.TransferInMediatorMessage(messageP) =>
        TransferInMediatorMessage.fromProtoV1(
          (hashOps, TargetProtocolVersion(expectedProtocolVersion))
        )(messageP)
      case Content.RootHashMessage(messageP) =>
        RootHashMessage.fromProtoV0(SerializedRootHashMessagePayload.fromByteString)(messageP)
      case Content.RegisterTopologyTransactionRequest(messageP) =>
        RegisterTopologyTransactionRequest.fromProtoV0(expectedProtocolVersion, messageP)
      case Content.RegisterTopologyTransactionResponse(messageP) =>
        RegisterTopologyTransactionResponse.fromProtoV1(messageP)
      case Content.CausalityMessage(messageP) => CausalityMessage.fromProtoV0(messageP)
      case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
    }): ParsingResult[ProtocolMessageV1]
    messageE.map(message => new EnvelopeContentV1(message) {})
  }

  private def fromProtoV2(
      context: (HashOps, ProtocolVersion),
      envelopeContent: v2.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    val (hashOps, expectedProtocolVersion) = context
    import v2.EnvelopeContent.SomeEnvelopeContent as Content
    val messageE = (envelopeContent.someEnvelopeContent match {
      case Content.InformeeMessage(messageP) =>
        InformeeMessage.fromProtoV1(context)(messageP)
      case Content.DomainTopologyTransactionMessage(messageP) =>
        DomainTopologyTransactionMessage.fromProtoV1(ProtoVersion(1))(
          expectedProtocolVersion,
          messageP,
        )
      case Content.EncryptedViewMessage(messageP) =>
        EncryptedViewMessageV1.fromProto(messageP)
      case Content.SignedMessage(messageP) =>
        SignedProtocolMessage.fromProtoV0(context, messageP)
      case Content.TransferOutMediatorMessage(messageP) =>
        TransferOutMediatorMessage.fromProtoV1(
          (hashOps, SourceProtocolVersion(expectedProtocolVersion))
        )(messageP)
      case Content.TransferInMediatorMessage(messageP) =>
        TransferInMediatorMessage.fromProtoV1(
          (hashOps, TargetProtocolVersion(expectedProtocolVersion))
        )(messageP)
      case Content.RootHashMessage(messageP) =>
        RootHashMessage.fromProtoV0(SerializedRootHashMessagePayload.fromByteString)(messageP)
      case Content.RegisterTopologyTransactionRequest(messageP) =>
        RegisterTopologyTransactionRequest.fromProtoV0(expectedProtocolVersion, messageP)
      case Content.RegisterTopologyTransactionResponse(messageP) =>
        RegisterTopologyTransactionResponse.fromProtoV1(messageP)
      case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
    }): ParsingResult[ProtocolMessageV2]
    messageE.map(message => new EnvelopeContentV2(message) {})
  }

  def fromProtoV3(
      context: (HashOps, ProtocolVersion),
      envelopeContent: v3.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    val (hashOps, expectedProtocolVersion) = context
    import v3.EnvelopeContent.SomeEnvelopeContent as Content
    val messageE = (envelopeContent.someEnvelopeContent match {
      case Content.InformeeMessage(messageP) =>
        InformeeMessage.fromProtoV1(context)(messageP)
      case Content.DomainTopologyTransactionMessage(messageP) =>
        DomainTopologyTransactionMessage.fromProtoV1(ProtoVersion(2))(
          expectedProtocolVersion,
          messageP,
        )
      case Content.EncryptedViewMessage(messageP) =>
        EncryptedViewMessageV2.fromProto(messageP)
      case Content.SignedMessage(messageP) =>
        SignedProtocolMessage.fromProtoV0(context, messageP)
      case Content.TransferOutMediatorMessage(messageP) =>
        TransferOutMediatorMessage.fromProtoV1(
          (hashOps, SourceProtocolVersion(expectedProtocolVersion))
        )(messageP)
      case Content.TransferInMediatorMessage(messageP) =>
        TransferInMediatorMessage.fromProtoV1(
          (hashOps, TargetProtocolVersion(expectedProtocolVersion))
        )(messageP)
      case Content.RootHashMessage(messageP) =>
        RootHashMessage.fromProtoV0(SerializedRootHashMessagePayload.fromByteString)(messageP)
      case Content.RegisterTopologyTransactionRequest(messageP) =>
        RegisterTopologyTransactionRequest.fromProtoV0(expectedProtocolVersion, messageP)
      case Content.RegisterTopologyTransactionResponse(messageP) =>
        RegisterTopologyTransactionResponse.fromProtoV1(messageP)
      case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
    }): ParsingResult[ProtocolMessageV3]
    messageE.map(message => new EnvelopeContentV3(message) {})
  }

  override def name: String = "EnvelopeContent"

  def messageFromByteArray[M <: UnsignedProtocolMessage](
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  )(
      bytes: Array[Byte]
  )(implicit cast: ProtocolMessageContentCast[M]): ParsingResult[M] =
    for {
      envelopeContent <- fromByteStringLegacy(protocolVersion)((hashOps, protocolVersion))(
        ByteString.copyFrom(bytes)
      )
      message <- cast
        .toKind(envelopeContent.message)
        .toRight(
          ProtoDeserializationError.OtherError(
            s"Cannot deserialize ${envelopeContent.message} as a ${cast.targetKind}"
          )
        )
    } yield message
}
