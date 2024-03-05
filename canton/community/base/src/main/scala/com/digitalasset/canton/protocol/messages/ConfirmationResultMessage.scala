// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, RootHash, v30}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Result message that the mediator sends to all stakeholders of a request with its verdict.
  *
  * @param domainId the domain on which the request is running
  * @param viewType determines which processor (transaction / transfer) must process this message
  * @param requestId unique identifier of the confirmation request
  * @param rootHashO hash over the contents of the request
  * @param verdict the finalized verdict on the request
  * @param informees of the request - empty for transactions
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class ConfirmationResultMessage private (
    override val domainId: DomainId,
    viewType: ViewType,
    override val requestId: RequestId,
    rootHashO: Option[RootHash],
    verdict: Verdict,
    informees: Set[LfPartyId],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ConfirmationResultMessage.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends ProtocolVersionedMemoizedEvidence
    with HasDomainId
    with HasRequestId
    with SignedProtocolMessageContent
    with HasProtocolVersionedWrapper[ConfirmationResultMessage]
    with PrettyPrinting {

  override def signingTimestamp: Option[CantonTimestamp] = Some(requestId.unwrap)

  def copy(
      domainId: DomainId = this.domainId,
      viewType: ViewType = this.viewType,
      requestId: RequestId = this.requestId,
      rootHashO: Option[RootHash] = this.rootHashO,
      verdict: Verdict = this.verdict,
      informees: Set[LfPartyId] = this.informees,
  ): ConfirmationResultMessage =
    ConfirmationResultMessage(domainId, viewType, requestId, rootHashO, verdict, informees)(
      representativeProtocolVersion,
      None,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  @transient override protected lazy val companionObj: ConfirmationResultMessage.type =
    ConfirmationResultMessage

  protected def toProtoV30: v30.ConfirmationResultMessage =
    v30.ConfirmationResultMessage(
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
      requestId = requestId.toProtoPrimitive,
      rootHash = rootHashO.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      verdict = Some(verdict.toProtoV30),
      informees = informees.toSeq,
    )

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.ConfirmationResult(
      getCryptographicEvidence
    )

  override def pretty: Pretty[ConfirmationResultMessage] =
    prettyOfClass(
      param("domainId", _.domainId),
      param("viewType", _.viewType),
      param("requestId", _.requestId.unwrap),
      param("rootHash", _.rootHashO),
      param("verdict", _.verdict),
      paramIfNonEmpty("informees", _.informees),
    )
}

object ConfirmationResultMessage
    extends HasMemoizedProtocolVersionedWrapperCompanion[
      ConfirmationResultMessage,
    ] {
  override val name: String = "ConfirmationResultMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v30.ConfirmationResultMessage
    )(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(
      domainId: DomainId,
      viewType: ViewType,
      requestId: RequestId,
      rootHashO: Option[RootHash],
      verdict: Verdict,
      informees: Set[LfPartyId],
      protocolVersion: ProtocolVersion,
  ): ConfirmationResultMessage =
    ConfirmationResultMessage(domainId, viewType, requestId, rootHashO, verdict, informees)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV30(protoResultMessage: v30.ConfirmationResultMessage)(
      bytes: ByteString
  ): ParsingResult[ConfirmationResultMessage] = {
    val v30.ConfirmationResultMessage(
      domainIdP,
      viewTypeP,
      requestIdP,
      rootHashP,
      verdictPO,
      informeesP,
    ) = protoResultMessage

    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      requestId <- RequestId.fromProtoPrimitive(requestIdP)
      rootHashO <- RootHash.fromProtoPrimitiveOption(rootHashP)
      verdict <- ProtoConverter.parseRequired(Verdict.fromProtoV30, "verdict", verdictPO)
      informees <- informeesP.traverse(ProtoConverter.parseLfPartyId)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield ConfirmationResultMessage(
      domainId,
      viewType,
      requestId,
      rootHashO,
      verdict,
      informees.toSet,
    )(rpv, Some(bytes))
  }

  implicit val confirmationResultMessageCast: SignedMessageContentCast[ConfirmationResultMessage] =
    SignedMessageContentCast.create[ConfirmationResultMessage]("ConfirmationResultMessage") {
      case m: ConfirmationResultMessage => Some(m)
      case _ => None
    }
}
