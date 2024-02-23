// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.TransferDomainId.TransferDomainIdCast
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Confirmation request result for a transfer request
  *
  * @param requestId timestamp of the corresponding [[TransferOutRequest]] on the source domain
  */
final case class TransferResult[+Domain <: TransferDomainId] private (
    override val requestId: RequestId,
    informees: Set[LfPartyId],
    domain: Domain, // For transfer-out, this is the source domain. For transfer-in, this is the target domain.
    override val verdict: Verdict,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransferResult.type],
    override val deserializedFrom: Option[ByteString],
) extends RegularConfirmationResult
    with HasProtocolVersionedWrapper[TransferResult[TransferDomainId]]
    with PrettyPrinting {

  override def domainId: DomainId = domain.unwrap

  override def viewType: ViewType = domain.toViewType

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.TransferResult(
      getCryptographicEvidence
    )

  @transient override protected lazy val companionObj: TransferResult.type = TransferResult

  private def toProtoV30: v30.TransferResult = {
    val domainP = (domain: @unchecked) match {
      case SourceDomainId(domainId) =>
        v30.TransferResult.Domain.SourceDomain(domainId.toProtoPrimitive)
      case TargetDomainId(domainId) =>
        v30.TransferResult.Domain.TargetDomain(domainId.toProtoPrimitive)
    }
    v30.TransferResult(
      requestId = Some(requestId.toProtoPrimitive),
      domain = domainP,
      informees = informees.toSeq,
      verdict = Some(verdict.toProtoV30),
    )
  }

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

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
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.TransferResult)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
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

  private def fromProtoV30(transferResultP: v30.TransferResult)(
      bytes: ByteString
  ): ParsingResult[TransferResult[TransferDomainId]] = {
    val v30.TransferResult(maybeRequestIdPO, domainP, informeesP, verdictPO) = transferResultP
    import v30.TransferResult.Domain
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
        .flatMap(Verdict.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield TransferResult(requestId, informees.toSet, domain, verdict)(rpv, Some(bytes))
  }

  implicit def transferResultCast[Kind <: TransferDomainId](implicit
      cast: TransferDomainIdCast[Kind]
  ): SignedMessageContentCast[TransferResult[Kind]] =
    SignedMessageContentCast.create[TransferResult[Kind]]("TransferResult") {
      case result: TransferResult[TransferDomainId] => result.traverse(cast.toKind)
      case _ => None
    }
}
