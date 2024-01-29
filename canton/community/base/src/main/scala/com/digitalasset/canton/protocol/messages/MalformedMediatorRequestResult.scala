// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.messages.Verdict.{
  MediatorRejectV0,
  MediatorRejectV1,
  MediatorRejectV2,
}
import com.digitalasset.canton.protocol.{RequestId, v0, v1, v2}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWrapperCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

import scala.Ordered.orderingToOrdered

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

  validateInstance().valueOr(err => throw new IllegalArgumentException(err))

  override def hashPurpose: HashPurpose = HashPurpose.MalformedMediatorRequestResult

  override protected[messages] def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.MalformedMediatorRequestResult(
      getCryptographicEvidence
    )

  @transient override protected lazy val companionObj: MalformedMediatorRequestResult.type =
    MalformedMediatorRequestResult

  private def mismatchingProtocolVersion: Nothing =
    throw new IllegalStateException(
      s"Mediator rejection's representative protocol version ${verdict.representativeProtocolVersion} cannot be used with" +
        s"${MalformedMediatorRequestResult.getClass.getSimpleName}'s representative protocol version $representativeProtocolVersion " +
        "by the class invariant"
    )

  protected def toProtoV0: v0.MalformedMediatorRequestResult = {
    val rejection = verdict match {
      case reject: Verdict.MediatorRejectV0 => reject.toProtoMediatorRejectionV0
      case _ => mismatchingProtocolVersion
    }
    v0.MalformedMediatorRequestResult(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
      rejection = Some(rejection),
    )
  }

  protected def toProtoV1: v1.MalformedMediatorRequestResult = {
    val rejection = verdict match {
      case reject: Verdict.MediatorRejectV1 => reject.toProtoMediatorRejectV1
      case _ => mismatchingProtocolVersion
    }
    v1.MalformedMediatorRequestResult(
      requestId = Some(requestId.toProtoPrimitive),
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
      rejection = Some(rejection),
    )
  }

  protected def toProtoV2: v2.MalformedMediatorRequestResult = {
    val rejection = verdict match {
      case reject: Verdict.MediatorRejectV2 => reject.toProtoMediatorRejectV2
      case _ => mismatchingProtocolVersion
    }
    v2.MalformedMediatorRequestResult(
      requestId = Some(requestId.toProtoPrimitive),
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
      rejection = Some(rejection),
    )
  }

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
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(
      v0.MalformedMediatorRequestResult
    )(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(
      v1.MalformedMediatorRequestResult
    )(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v6)(
      v2.MalformedMediatorRequestResult
    )(
      supportedProtoVersionMemoized(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
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

  lazy val mediatorRejectVersionInvariant = new Invariant {
    override def validateInstance(
        v: MalformedMediatorRequestResult,
        rpv: RepresentativeProtocolVersion[MalformedMediatorRequestResult.type],
    ): Either[String, Unit] = {
      v.verdict match {
        case _: Verdict.MediatorRejectV0 =>
          EitherUtil.condUnitE(
            rpv == protocolVersionRepresentativeFor(
              MediatorRejectV0.applicableProtocolVersion
            ),
            Verdict.MediatorRejectV0.wrongProtocolVersion(rpv),
          )
        case _: Verdict.MediatorRejectV1 =>
          EitherUtil.condUnitE(
            rpv >= protocolVersionRepresentativeFor(
              MediatorRejectV1.firstApplicableProtocolVersion
            ) &&
              rpv <= protocolVersionRepresentativeFor(
                MediatorRejectV1.lastApplicableProtocolVersion
              ),
            Verdict.MediatorRejectV1.wrongProtocolVersion(rpv),
          )
        case _: Verdict.MediatorRejectV2 =>
          EitherUtil.condUnitE(
            rpv >= protocolVersionRepresentativeFor(
              MediatorRejectV2.firstApplicableProtocolVersion
            ),
            Verdict.MediatorRejectV2.wrongProtocolVersion(rpv),
          )
      }
    }
  }

  override def invariants: Seq[canton.protocol.messages.MalformedMediatorRequestResult.Invariant] =
    Seq(mediatorRejectVersionInvariant)

  private def fromProtoV0(protoResultMsg: v0.MalformedMediatorRequestResult)(
      bytes: ByteString
  ): ParsingResult[MalformedMediatorRequestResult] = {

    val v0.MalformedMediatorRequestResult(requestIdP, domainIdP, viewTypeP, rejectP) =
      protoResultMsg
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
        .map(RequestId(_))
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      reject <- ProtoConverter.parseRequired(MediatorRejectV0.fromProtoV0, "rejection", rejectP)
    } yield MalformedMediatorRequestResult(requestId, domainId, viewType, reject)(
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )
  }

  private def fromProtoV1(malformedMediatorRequestResultP: v1.MalformedMediatorRequestResult)(
      bytes: ByteString
  ): ParsingResult[MalformedMediatorRequestResult] = {

    val v1.MalformedMediatorRequestResult(requestIdPO, domainIdP, viewTypeP, rejectionPO) =
      malformedMediatorRequestResultP
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      reject <- ProtoConverter.parseRequired(
        MediatorRejectV1.fromProtoV1(_, Verdict.protocolVersionRepresentativeFor(ProtoVersion(1))),
        "rejection",
        rejectionPO,
      )
    } yield MalformedMediatorRequestResult(requestId, domainId, viewType, reject)(
      protocolVersionRepresentativeFor(ProtoVersion(1)),
      Some(bytes),
    )
  }

  private def fromProtoV2(malformedMediatorRequestResultP: v2.MalformedMediatorRequestResult)(
      bytes: ByteString
  ): ParsingResult[MalformedMediatorRequestResult] = {

    val v2.MalformedMediatorRequestResult(requestIdPO, domainIdP, viewTypeP, rejectionPO) =
      malformedMediatorRequestResultP
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      reject <- ProtoConverter.parseRequired(
        MediatorRejectV2.fromProtoV2,
        "rejection",
        rejectionPO,
      )
    } yield MalformedMediatorRequestResult(requestId, domainId, viewType, reject)(
      protocolVersionRepresentativeFor(ProtoVersion(2)),
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
