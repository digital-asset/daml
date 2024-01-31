// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.crypto.HashPurpose
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

/** Sent by the mediator to indicate that a mediator request was malformed.
  * The request counts as being rejected and the request UUID will not be deduplicated.
  *
  * @param requestId The ID of the malformed request
  * @param domainId The domain ID of the mediator
  * @param verdict The rejection reason as a verdict
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class MalformedMediatorRequestResult private (
    override val requestId: RequestId,
    override val domainId: DomainId,
    override val viewType: ViewType,
    override val verdict: Verdict.MediatorReject,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      MalformedMediatorRequestResult.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MediatorResult
    with SignedProtocolMessageContent
    with HasProtocolVersionedWrapper[MalformedMediatorRequestResult]
    with PrettyPrinting {

  override def hashPurpose: HashPurpose = HashPurpose.MalformedMediatorRequestResult

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.MalformedMediatorRequestResult(
      getCryptographicEvidence
    )

  @transient override protected lazy val companionObj: MalformedMediatorRequestResult.type =
    MalformedMediatorRequestResult

  protected def toProtoV30: v30.MalformedMediatorRequestResult =
    v30.MalformedMediatorRequestResult(
      requestId = Some(requestId.toProtoPrimitive),
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
      rejection = Some(verdict.toProtoMediatorRejectV30),
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[MalformedMediatorRequestResult] = prettyOfClass(
    param("request id", _.requestId),
    param("reject", _.verdict),
    param("view type", _.viewType),
    param("domain id", _.domainId),
  )
}

object MalformedMediatorRequestResult
    extends HasMemoizedProtocolVersionedWrapperCompanion[MalformedMediatorRequestResult] {
  override val name: String = "MalformedMediatorRequestResult"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v30.MalformedMediatorRequestResult
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
  ): MalformedMediatorRequestResult =
    MalformedMediatorRequestResult(requestId, domainId, viewType, verdict)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  def create(
      requestId: RequestId,
      domainId: DomainId,
      viewType: ViewType,
      verdict: Verdict.MediatorReject,
      protocolVersion: ProtocolVersion,
  ): Either[String, MalformedMediatorRequestResult] =
    Either
      .catchOnly[IllegalArgumentException](
        MalformedMediatorRequestResult
          .tryCreate(requestId, domainId, viewType, verdict, protocolVersion)
      )
      .leftMap(_.getMessage)

  private def fromProtoV30(malformedMediatorRequestResultP: v30.MalformedMediatorRequestResult)(
      bytes: ByteString
  ): ParsingResult[MalformedMediatorRequestResult] = {

    val v30.MalformedMediatorRequestResult(requestIdPO, domainIdP, viewTypeP, rejectionPO) =
      malformedMediatorRequestResultP
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
    } yield MalformedMediatorRequestResult(requestId, domainId, viewType, reject)(
      rpv,
      Some(bytes),
    )
  }

  implicit val malformedMediatorRequestResultCast
      : SignedMessageContentCast[MalformedMediatorRequestResult] = SignedMessageContentCast
    .create[MalformedMediatorRequestResult]("MalformedMediatorRequestResult") {
      case m: MalformedMediatorRequestResult => Some(m)
      case _ => None
    }
}
