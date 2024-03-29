// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.Transfer.TargetProtocolVersion
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

final case class EnvelopeContent(message: UnsignedProtocolMessage)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent.type]
) extends HasProtocolVersionedWrapper[EnvelopeContent] {
  @transient override protected lazy val companionObj: EnvelopeContent.type = EnvelopeContent

  def toByteStringUnversioned: ByteString =
    v30.EnvelopeContent(message.toProtoSomeEnvelopeContentV30).toByteString
}

object EnvelopeContent
    extends HasProtocolVersionedWithContextAndValidationCompanion[EnvelopeContent, HashOps] {

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(
      ProtocolVersion.v30
    )(v30.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toByteStringUnversioned,
    )
  )

  def create(
      message: ProtocolMessage,
      protocolVersion: ProtocolVersion,
  ): Either[String, EnvelopeContent] = {
    val representativeProtocolVersion = protocolVersionRepresentativeFor(protocolVersion)
    message match {
      case messageV4: UnsignedProtocolMessage =>
        Right(EnvelopeContent(messageV4)(representativeProtocolVersion))
      case _ =>
        Left(s"Cannot use message $message in protocol version $protocolVersion")
    }
  }

  def tryCreate(
      message: ProtocolMessage,
      protocolVersion: ProtocolVersion,
  ): EnvelopeContent =
    create(message, protocolVersion).valueOr(err => throw new IllegalArgumentException(err))

  private def fromProtoV30(
      context: (HashOps, ProtocolVersion),
      contentP: v30.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    val (hashOps, expectedProtocolVersion) = context
    import v30.EnvelopeContent.SomeEnvelopeContent as Content
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      content <- (contentP.someEnvelopeContent match {
        case Content.InformeeMessage(messageP) =>
          InformeeMessage.fromProtoV30(context)(messageP)
        case Content.EncryptedViewMessage(messageP) =>
          EncryptedViewMessage.fromProto(messageP)
        case Content.TransferOutMediatorMessage(messageP) =>
          TransferOutMediatorMessage.fromProtoV30(context)(messageP)
        case Content.TransferInMediatorMessage(messageP) =>
          TransferInMediatorMessage.fromProtoV30(
            (hashOps, TargetProtocolVersion(expectedProtocolVersion))
          )(messageP)
        case Content.RootHashMessage(messageP) =>
          RootHashMessage.fromProtoV30(SerializedRootHashMessagePayload.fromByteString)(messageP)
        case Content.TopologyTransactionsBroadcast(messageP) =>
          TopologyTransactionsBroadcastX.fromProtoV30(expectedProtocolVersion, messageP)
        case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
      }): ParsingResult[UnsignedProtocolMessage]
    } yield EnvelopeContent(content)(rpv)
  }

  override def name: String = "EnvelopeContent"

  def messageFromByteArray[M <: UnsignedProtocolMessage](
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  )(
      bytes: Array[Byte]
  )(implicit cast: ProtocolMessageContentCast[M]): ParsingResult[M] = {
    for {
      envelopeContent <- fromByteString(hashOps, protocolVersion)(ByteString.copyFrom(bytes))
      message <- cast
        .toKind(envelopeContent.message)
        .toRight(
          ProtoDeserializationError.OtherError(
            s"Cannot deserialize ${envelopeContent.message} as a ${cast.targetKind}"
          )
        )
    } yield message
  }
}
