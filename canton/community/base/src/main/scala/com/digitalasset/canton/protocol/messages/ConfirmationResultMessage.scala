// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, RootHash, v30}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Transaction result message that the mediator sends to all stakeholders of a transaction confirmation request with its verdict.
  * https://engineering.da-int.net/docs/platform-architecture-handbook/arch/canton/transactions.html#phase-6-broadcast-of-result
  *
  * @param requestId        identifier of the confirmation request
  * @param verdict          the finalized verdict on the request
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class ConfirmationResultMessage private (
    override val requestId: RequestId,
    override val verdict: Verdict,
    rootHash: RootHash,
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ConfirmationResultMessage.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends RegularConfirmationResult
    with HasProtocolVersionedWrapper[ConfirmationResultMessage]
    with PrettyPrinting {

  def copy(
      requestId: RequestId = this.requestId,
      verdict: Verdict = this.verdict,
      rootHash: RootHash = this.rootHash,
      domainId: DomainId = this.domainId,
  ): ConfirmationResultMessage =
    ConfirmationResultMessage(requestId, verdict, rootHash, domainId)(
      representativeProtocolVersion,
      None,
    )

  override def viewType: TransactionViewType = TransactionViewType

  /** Computes the serialization of the object as a [[com.google.protobuf.ByteString]].
    *
    * Must meet the contract of [[com.digitalasset.canton.serialization.HasCryptographicEvidence.getCryptographicEvidence]]
    * except that when called several times, different [[com.google.protobuf.ByteString]]s may be returned.
    */
  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  @transient override protected lazy val companionObj: ConfirmationResultMessage.type =
    ConfirmationResultMessage

  protected def toProtoV30: v30.TransactionResultMessage =
    v30.TransactionResultMessage(
      requestId = Some(requestId.toProtoPrimitive),
      verdict = Some(verdict.toProtoV30),
      rootHash = rootHash.toProtoPrimitive,
      domainId = domainId.toProtoPrimitive,
    )

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.TransactionResult(
      getCryptographicEvidence
    )

  override def pretty: Pretty[ConfirmationResultMessage] =
    prettyOfClass(
      param("requestId", _.requestId.unwrap),
      param("verdict", _.verdict),
      param("rootHash", _.rootHash),
      param("domainId", _.domainId),
    )
}

object ConfirmationResultMessage
    extends HasMemoizedProtocolVersionedWrapperCompanion[
      ConfirmationResultMessage,
    ] {
  override val name: String = "TransactionResultMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v30.TransactionResultMessage
    )(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def apply(
      requestId: RequestId,
      verdict: Verdict,
      rootHash: RootHash,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): ConfirmationResultMessage =
    ConfirmationResultMessage(requestId, verdict, rootHash, domainId)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV30(protoResultMessage: v30.TransactionResultMessage)(
      bytes: ByteString
  ): ParsingResult[ConfirmationResultMessage] = {
    val v30.TransactionResultMessage(requestIdPO, verdictPO, rootHashP, domainIdP) =
      protoResultMessage
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      transactionResult <- ProtoConverter
        .required("verdict", verdictPO)
        .flatMap(Verdict.fromProtoV30)
      rootHash <- RootHash.fromProtoPrimitive(rootHashP)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield ConfirmationResultMessage(requestId, transactionResult, rootHash, domainId)(
      rpv,
      Some(bytes),
    )
  }

  implicit val transactionResultMessageCast: SignedMessageContentCast[ConfirmationResultMessage] =
    SignedMessageContentCast.create[ConfirmationResultMessage]("TransactionResultMessage") {
      case m: ConfirmationResultMessage => Some(m)
      case _ => None
    }
}
