// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.{RequestId, v30}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWrapperCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

/** Sent by the mediator to indicate that a mediator confirmation request was malformed.
  * The request counts as being rejected and the request UUID will not be deduplicated.
  *
  * @param requestId The ID of the malformed request
  * @param domainId The domain ID of the mediator
  * @param verdict The rejection reason as a verdict
  */
final case class MalformedConfirmationRequestResult private (
    override val requestId: RequestId,
    override val domainId: DomainId,
    override val viewType: ViewType,
    override val verdict: Verdict.MediatorReject,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      MalformedConfirmationRequestResult.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends ConfirmationResult
    with SignedProtocolMessageContent
    with HasProtocolVersionedWrapper[MalformedConfirmationRequestResult]
    with PrettyPrinting {

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage
      .MalformedMediatorConfirmationRequestResult(
        getCryptographicEvidence
      )

  @transient override protected lazy val companionObj: MalformedConfirmationRequestResult.type =
    MalformedConfirmationRequestResult

  protected def toProtoV30: v30.MalformedMediatorConfirmationRequestResult =
    v30.MalformedMediatorConfirmationRequestResult(
      requestId = Some(requestId.toProtoPrimitive),
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
      rejection = Some(verdict.toProtoMediatorRejectV30),
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[MalformedConfirmationRequestResult] = prettyOfClass(
    param("request id", _.requestId),
    param("reject", _.verdict),
    param("view type", _.viewType),
    param("domain id", _.domainId),
  )
}

object MalformedConfirmationRequestResult
    extends HasMemoizedProtocolVersionedWrapperCompanion[MalformedConfirmationRequestResult] {
  override val name: String = "MalformedMediatorConfirmationRequestResult"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v30.MalformedMediatorConfirmationRequestResult
    )(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def tryCreate(
      requestId: RequestId,
      domainId: DomainId,
      viewType: ViewType,
      verdict: Verdict.MediatorReject,
      protocolVersion: ProtocolVersion,
  ): MalformedConfirmationRequestResult =
    MalformedConfirmationRequestResult(requestId, domainId, viewType, verdict)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  def create(
      requestId: RequestId,
      domainId: DomainId,
      viewType: ViewType,
      verdict: Verdict.MediatorReject,
      protocolVersion: ProtocolVersion,
  ): Either[String, MalformedConfirmationRequestResult] =
    Either
      .catchOnly[IllegalArgumentException](
        MalformedConfirmationRequestResult
          .tryCreate(requestId, domainId, viewType, verdict, protocolVersion)
      )
      .leftMap(_.getMessage)

  private def fromProtoV30(
      MalformedMediatorConfirmationRequestResultP: v30.MalformedMediatorConfirmationRequestResult
  )(
      bytes: ByteString
  ): ParsingResult[MalformedConfirmationRequestResult] = {

    val v30.MalformedMediatorConfirmationRequestResult(
      requestIdPO,
      domainIdP,
      viewTypeP,
      rejectionPO,
    ) =
      MalformedMediatorConfirmationRequestResultP
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      reject <- ProtoConverter.parseRequired(
        MediatorReject.fromProtoV30,
        "rejection",
        rejectionPO,
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield MalformedConfirmationRequestResult(requestId, domainId, viewType, reject)(
      rpv,
      Some(bytes),
    )
  }

  implicit val MalformedMediatorConfirmationRequestResultCast
      : SignedMessageContentCast[MalformedConfirmationRequestResult] = SignedMessageContentCast
    .create[MalformedConfirmationRequestResult]("MalformedMediatorConfirmationRequestResult") {
      case m: MalformedConfirmationRequestResult => Some(m)
      case _ => None
    }
}
