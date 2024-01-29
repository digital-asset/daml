// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.TransferDomainId.TransferDomainIdCast
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult.InvalidTransferOutResult
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{
  RequestId,
  SourceDomainId,
  TargetDomainId,
  TransferDomainId,
  TransferId,
  v0,
  v1,
  v2,
  v3,
}
import com.digitalasset.canton.sequencing.RawProtocolEvent
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, EventWithErrors, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Mediator result for a transfer request
  *
  * @param requestId timestamp of the corresponding [[TransferOutRequest]] on the source domain
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class TransferResult[+Domain <: TransferDomainId] private (
    override val requestId: RequestId,
    informees: Set[LfPartyId],
    domain: Domain, // For transfer-out, this is the source domain. For transfer-in, this is the target domain.
    override val verdict: Verdict,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransferResult.type],
    override val deserializedFrom: Option[ByteString],
) extends RegularMediatorResult
    with HasProtocolVersionedWrapper[TransferResult[TransferDomainId]]
    with PrettyPrinting {

  override def domainId: DomainId = domain.unwrap

  override def viewType: ViewType = domain.toViewType

  override protected[messages] def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage.TransferResult =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.TransferResult(getCryptographicEvidence)

  @transient override protected lazy val companionObj: TransferResult.type = TransferResult

  private def toProtoV0: v0.TransferResult = {
    val domainP = (domain: @unchecked) match {
      case SourceDomainId(domainId) =>
        v0.TransferResult.Domain.OriginDomain(domainId.toProtoPrimitive)
      case TargetDomainId(domainId) =>
        v0.TransferResult.Domain.TargetDomain(domainId.toProtoPrimitive)
    }
    v0.TransferResult(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      domain = domainP,
      informees = informees.toSeq,
      verdict = Some(verdict.toProtoV0),
    )
  }

  private def toProtoV1: v1.TransferResult = {
    val domainP = (domain: @unchecked) match {
      case SourceDomainId(domainId) =>
        v1.TransferResult.Domain.OriginDomain(domainId.toProtoPrimitive)
      case TargetDomainId(domainId) =>
        v1.TransferResult.Domain.TargetDomain(domainId.toProtoPrimitive)
    }
    v1.TransferResult(
      requestId = Some(requestId.toProtoPrimitive),
      domain = domainP,
      informees = informees.toSeq,
      verdict = Some(verdict.toProtoV1),
    )
  }

  private def toProtoV2: v2.TransferResult = {
    val domainP = (domain: @unchecked) match {
      case SourceDomainId(domainId) =>
        v2.TransferResult.Domain.SourceDomain(domainId.toProtoPrimitive)
      case TargetDomainId(domainId) =>
        v2.TransferResult.Domain.TargetDomain(domainId.toProtoPrimitive)
    }
    v2.TransferResult(
      requestId = Some(requestId.toProtoPrimitive),
      domain = domainP,
      informees = informees.toSeq,
      verdict = Some(verdict.toProtoV2),
    )
  }

  private def toProtoV3: v3.TransferResult = {
    val domainP = (domain: @unchecked) match {
      case SourceDomainId(domainId) =>
        v3.TransferResult.Domain.SourceDomain(domainId.toProtoPrimitive)
      case TargetDomainId(domainId) =>
        v3.TransferResult.Domain.TargetDomain(domainId.toProtoPrimitive)
    }
    v3.TransferResult(
      requestId = Some(requestId.toProtoPrimitive),
      domain = domainP,
      informees = informees.toSeq,
      verdict = Some(verdict.toProtoV3),
    )
  }

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.TransferResultSignature

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[TransferResult] def traverse[F[_], Domain2 <: TransferDomainId](
      f: Domain => F[Domain2]
  )(implicit F: Functor[F]): F[TransferResult[Domain2]] =
    F.map(f(domain)) { newDomain =>
      if (newDomain eq domain) this.asInstanceOf[TransferResult[Domain2]]
      else if (newDomain == domain)
        TransferResult(requestId, informees, newDomain, verdict)(
          representativeProtocolVersion,
          deserializedFrom,
        )
      else
        TransferResult(requestId, informees, newDomain, verdict)(
          representativeProtocolVersion,
          None,
        )
    }

  override def pretty: Pretty[TransferResult[_ <: TransferDomainId]] =
    prettyOfClass(
      param("requestId", _.requestId.unwrap),
      param("verdict", _.verdict),
      param("informees", _.informees),
      param("domain", _.domain),
    )
}

object TransferResult
    extends HasMemoizedProtocolVersionedWrapperCompanion[TransferResult[TransferDomainId]] {
  override val name: String = "TransferResult"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransferResult)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransferResult)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v5)(v2.TransferResult)(
      supportedProtoVersionMemoized(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
    ProtoVersion(3) -> VersionedProtoConverter(ProtocolVersion.v6)(v3.TransferResult)(
      supportedProtoVersionMemoized(_)(fromProtoV3),
      _.toProtoV3.toByteString,
    ),
  )

  def create[Domain <: TransferDomainId](
      requestId: RequestId,
      informees: Set[LfPartyId],
      domain: Domain,
      verdict: Verdict,
      protocolVersion: ProtocolVersion,
  ): TransferResult[Domain] =
    TransferResult[Domain](requestId, informees, domain, verdict)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV0(transferResultP: v0.TransferResult)(
      bytes: ByteString
  ): ParsingResult[TransferResult[TransferDomainId]] =
    transferResultP match {
      case v0.TransferResult(maybeRequestIdP, domainP, informeesP, maybeVerdictP) =>
        import v0.TransferResult.Domain
        for {
          requestId <- ProtoConverter
            .required("TransferOutResult.requestId", maybeRequestIdP)
            .flatMap(CantonTimestamp.fromProtoPrimitive)
            .map(RequestId(_))
          domain <- domainP match {
            case Domain.OriginDomain(sourceDomain) =>
              DomainId
                .fromProtoPrimitive(sourceDomain, "TransferResult.originDomain")
                .map(SourceDomainId(_))
            case Domain.TargetDomain(targetDomain) =>
              DomainId
                .fromProtoPrimitive(targetDomain, "TransferResult.targetDomain")
                .map(TargetDomainId(_))
            case Domain.Empty => Left(FieldNotSet("TransferResponse.domain"))
          }
          informees <- informeesP.traverse(ProtoConverter.parseLfPartyId)
          verdict <- ProtoConverter
            .required("TransferResult.verdict", maybeVerdictP)
            .flatMap(Verdict.fromProtoV0)
        } yield TransferResult(requestId, informees.toSet, domain, verdict)(
          protocolVersionRepresentativeFor(ProtoVersion(0)),
          Some(bytes),
        )
    }

  private def fromProtoV1(transferResultP: v1.TransferResult)(
      bytes: ByteString
  ): ParsingResult[TransferResult[TransferDomainId]] = {
    val v1.TransferResult(maybeRequestIdPO, domainP, informeesP, verdictPO) = transferResultP
    import v1.TransferResult.Domain
    for {
      requestId <- ProtoConverter
        .required("TransferOutResult.requestId", maybeRequestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      domain <- domainP match {
        case Domain.OriginDomain(sourceDomain) =>
          DomainId
            .fromProtoPrimitive(sourceDomain, "TransferResult.originDomain")
            .map(SourceDomainId(_))
        case Domain.TargetDomain(targetDomain) =>
          DomainId
            .fromProtoPrimitive(targetDomain, "TransferResult.targetDomain")
            .map(TargetDomainId(_))
        case Domain.Empty => Left(FieldNotSet("TransferResponse.domain"))
      }
      informees <- informeesP.traverse(ProtoConverter.parseLfPartyId)
      verdict <- ProtoConverter
        .required("TransferResult.verdict", verdictPO)
        .flatMap(Verdict.fromProtoV1)
    } yield TransferResult(requestId, informees.toSet, domain, verdict)(
      protocolVersionRepresentativeFor(ProtoVersion(1)),
      Some(bytes),
    )
  }

  private def fromProtoV2(transferResultP: v2.TransferResult)(
      bytes: ByteString
  ): ParsingResult[TransferResult[TransferDomainId]] = {
    val v2.TransferResult(maybeRequestIdPO, domainP, informeesP, verdictPO) = transferResultP
    import v2.TransferResult.Domain
    for {
      requestId <- ProtoConverter
        .required("TransferOutResult.requestId", maybeRequestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      domain <- domainP match {
        case Domain.SourceDomain(sourceDomain) =>
          DomainId
            .fromProtoPrimitive(sourceDomain, "TransferResult.sourceDomain")
            .map(SourceDomainId(_))
        case Domain.TargetDomain(targetDomain) =>
          DomainId
            .fromProtoPrimitive(targetDomain, "TransferResult.targetDomain")
            .map(TargetDomainId(_))
        case Domain.Empty => Left(FieldNotSet("TransferResponse.domain"))
      }
      informees <- informeesP.traverse(ProtoConverter.parseLfPartyId)
      verdict <- ProtoConverter
        .required("TransferResult.verdict", verdictPO)
        .flatMap(Verdict.fromProtoV2)
    } yield TransferResult(requestId, informees.toSet, domain, verdict)(
      protocolVersionRepresentativeFor(ProtoVersion(2)),
      Some(bytes),
    )
  }

  private def fromProtoV3(transferResultP: v3.TransferResult)(
      bytes: ByteString
  ): ParsingResult[TransferResult[TransferDomainId]] = {
    val v3.TransferResult(maybeRequestIdPO, domainP, informeesP, verdictPO) = transferResultP
    import v3.TransferResult.Domain
    for {
      requestId <- ProtoConverter
        .required("TransferOutResult.requestId", maybeRequestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      domain <- domainP match {
        case Domain.SourceDomain(sourceDomain) =>
          DomainId
            .fromProtoPrimitive(sourceDomain, "TransferResult.sourceDomain")
            .map(SourceDomainId(_))
        case Domain.TargetDomain(targetDomain) =>
          DomainId
            .fromProtoPrimitive(targetDomain, "TransferResult.targetDomain")
            .map(TargetDomainId(_))
        case Domain.Empty => Left(FieldNotSet("TransferResponse.domain"))
      }
      informees <- informeesP.traverse(ProtoConverter.parseLfPartyId)
      verdict <- ProtoConverter
        .required("TransferResult.verdict", verdictPO)
        .flatMap(Verdict.fromProtoV3)
    } yield TransferResult(requestId, informees.toSet, domain, verdict)(
      protocolVersionRepresentativeFor(ProtoVersion(3)),
      Some(bytes),
    )
  }

  implicit def transferResultCast[Kind <: TransferDomainId](implicit
      cast: TransferDomainIdCast[Kind]
  ): SignedMessageContentCast[TransferResult[Kind]] =
    SignedMessageContentCast.create[TransferResult[Kind]]("TransferResult") {
      case result: TransferResult[TransferDomainId] => result.traverse(cast.toKind)
      case _ => None
    }
}

final case class DeliveredTransferOutResult(result: SignedContent[Deliver[DefaultOpenEnvelope]])
    extends PrettyPrinting {

  val unwrap: TransferOutResult = result.content match {
    case Deliver(_, _, _, _, Batch(envelopes)) =>
      val transferOutResults =
        envelopes.mapFilter(ProtocolMessage.select[SignedProtocolMessage[TransferOutResult]])
      val size = transferOutResults.size
      if (size != 1)
        throw InvalidTransferOutResult(
          result.content,
          s"The deliver event must contain exactly one transfer-out result, but found $size.",
        )
      transferOutResults(0).protocolMessage.message
  }

  unwrap.verdict match {
    case _: Verdict.Approve => ()
    case _: Verdict.MediatorReject | _: Verdict.ParticipantReject =>
      throw InvalidTransferOutResult(result.content, "The transfer-out result must be approving.")
  }

  def transferId: TransferId = TransferId(unwrap.domain, unwrap.requestId.unwrap)

  override def pretty: Pretty[DeliveredTransferOutResult] = prettyOfParam(_.unwrap)
}

object DeliveredTransferOutResult {

  final case class InvalidTransferOutResult(
      transferOutResult: RawProtocolEvent,
      message: String,
  ) extends RuntimeException(s"$message: $transferOutResult")

  def create(
      resultE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[RawProtocolEvent],
      ]
  ): Either[InvalidTransferOutResult, DeliveredTransferOutResult] =
    for {
      // The event signature would be invalid if some envelopes could not be opened upstream.
      // However, this should not happen, because transfer out messages are sent by the mediator,
      // who is trusted not to send bad envelopes.
      result <- resultE match {
        case Left(eventWithErrors) =>
          Left(
            InvalidTransferOutResult(
              eventWithErrors.content,
              "Result event contains envelopes that could not be deserialized.",
            )
          )
        case Right(event) => Right(event)
      }
      castToDeliver <- result
        .traverse(Deliver.fromSequencedEvent)
        .toRight(
          InvalidTransferOutResult(
            result.content,
            "Only a Deliver event contains a transfer-out result.",
          )
        )
      deliveredTransferOutResult <- Either.catchOnly[InvalidTransferOutResult] {
        DeliveredTransferOutResult(castToDeliver)
      }
    } yield deliveredTransferOutResult
}
