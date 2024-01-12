// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.bifunctor.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.{HashOps, HashPurpose}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{CantonTimestamp, InformeeTree}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, RootHash, v0, v1, v2, v3}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Transaction result message that the mediator sends to all stakeholders of a confirmation request with its verdict.
  * https://engineering.da-int.net/docs/platform-architecture-handbook/arch/canton/transactions.html#phase-6-broadcast-of-result
  *
  * @param requestId        identifier of the confirmation request
  * @param verdict          the finalized verdict on the request
  * @param notificationTree the informee tree unblinded for the parties hosted by the receiving participant
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class TransactionResultMessage private (
    override val requestId: RequestId,
    override val verdict: Verdict,
    rootHash: RootHash,
    override val domainId: DomainId,
    notificationTree: Option[InformeeTree], // TODO(i12171): remove in 3.0
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TransactionResultMessage.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends RegularMediatorResult
    with HasProtocolVersionedWrapper[TransactionResultMessage]
    with PrettyPrinting {

  def copy(
      requestId: RequestId = this.requestId,
      verdict: Verdict = this.verdict,
      rootHash: RootHash = this.rootHash,
      domainId: DomainId = this.domainId,
      notificationTree: Option[InformeeTree] = this.notificationTree,
  ): TransactionResultMessage =
    TransactionResultMessage(requestId, verdict, rootHash, domainId, notificationTree)(
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

  @transient override protected lazy val companionObj: TransactionResultMessage.type =
    TransactionResultMessage

  protected def toProtoV0: v0.TransactionResultMessage =
    v0.TransactionResultMessage(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      verdict = Some(verdict.toProtoV0),
      notificationTree = notificationTree.map(_.toProtoV0),
    )

  protected def toProtoV1: v1.TransactionResultMessage = v1.TransactionResultMessage(
    requestId = Some(requestId.toProtoPrimitive),
    verdict = Some(verdict.toProtoV1),
    notificationTree = notificationTree.map(_.toProtoV1),
  )

  protected def toProtoV2: v2.TransactionResultMessage =
    v2.TransactionResultMessage(
      requestId = Some(requestId.toProtoPrimitive),
      verdict = Some(verdict.toProtoV2),
      rootHash = rootHash.toProtoPrimitive,
      domainId = domainId.toProtoPrimitive,
    )

  protected def toProtoV3: v3.TransactionResultMessage =
    v3.TransactionResultMessage(
      requestId = Some(requestId.toProtoPrimitive),
      verdict = Some(verdict.toProtoV3),
      rootHash = rootHash.toProtoPrimitive,
      domainId = domainId.toProtoPrimitive,
    )

  override protected[messages] def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage.TransactionResult =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.TransactionResult(getCryptographicEvidence)

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v0.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v0.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.TransactionResult(
      getCryptographicEvidence
    )

  override def hashPurpose: HashPurpose = HashPurpose.TransactionResultSignature

  override def pretty: Pretty[TransactionResultMessage] =
    prettyOfClass(
      param("requestId", _.requestId.unwrap),
      param("verdict", _.verdict),
      param("rootHash", _.rootHash),
      param("domainId", _.domainId),
    )
}

object TransactionResultMessage
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      TransactionResultMessage,
      (HashOps, ProtocolVersion),
    ] {
  override val name: String = "TransactionResultMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransactionResultMessage)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransactionResultMessage)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v5)(v2.TransactionResultMessage)(
      supportedProtoVersionMemoized(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
    ProtoVersion(3) -> VersionedProtoConverter(ProtocolVersion.v6)(v3.TransactionResultMessage)(
      supportedProtoVersionMemoized(_)(fromProtoV3),
      _.toProtoV3.toByteString,
    ),
  )

  def apply(
      requestId: RequestId,
      verdict: Verdict,
      rootHash: RootHash,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): TransactionResultMessage =
    TransactionResultMessage(requestId, verdict, rootHash, domainId, None)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  // TODO(i12171): Remove in 3.0 and drop context HashOps
  def apply(
      requestId: RequestId,
      verdict: Verdict,
      notificationTree: InformeeTree,
      protocolVersion: ProtocolVersion,
  ): TransactionResultMessage =
    TransactionResultMessage(
      requestId,
      verdict,
      notificationTree.tree.rootHash,
      notificationTree.domainId,
      Some(notificationTree),
    )(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV0(
      context: (HashOps, ProtocolVersion),
      protoResultMessage: v0.TransactionResultMessage,
  )(
      bytes: ByteString
  ): ParsingResult[TransactionResultMessage] =
    for {
      requestId <- ProtoConverter
        .required("request_id", protoResultMessage.requestId)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
        .map(RequestId(_))
      transactionResult <- ProtoConverter
        .required("verdict", protoResultMessage.verdict)
        .flatMap(Verdict.fromProtoV0)
      protoNotificationTree <- ProtoConverter
        .required("notification_tree", protoResultMessage.notificationTree)
        .leftWiden[ProtoDeserializationError]
      notificationTree <- InformeeTree.fromProtoV0(context, protoNotificationTree)
    } yield TransactionResultMessage(
      requestId,
      transactionResult,
      notificationTree.tree.rootHash,
      notificationTree.domainId,
      Some(notificationTree),
    )(
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )

  private def fromProtoV1(
      context: (HashOps, ProtocolVersion),
      protoResultMessage: v1.TransactionResultMessage,
  )(
      bytes: ByteString
  ): ParsingResult[TransactionResultMessage] = {
    val v1.TransactionResultMessage(requestIdPO, verdictPO, notificationTreePO) = protoResultMessage
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      transactionResult <- ProtoConverter
        .required("verdict", verdictPO)
        .flatMap(Verdict.fromProtoV1)
      notificationTree <- ProtoConverter
        .required("notification_tree", notificationTreePO)
        .flatMap(InformeeTree.fromProtoV1(context, _))
    } yield TransactionResultMessage(
      requestId,
      transactionResult,
      notificationTree.tree.rootHash,
      notificationTree.domainId,
      Some(notificationTree),
    )(
      protocolVersionRepresentativeFor(ProtoVersion(1)),
      Some(bytes),
    )
  }

  private def fromProtoV2(
      _context: (HashOps, ProtocolVersion),
      protoResultMessage: v2.TransactionResultMessage,
  )(
      bytes: ByteString
  ): ParsingResult[TransactionResultMessage] = {
    val v2.TransactionResultMessage(requestIdPO, verdictPO, rootHashP, domainIdP) =
      protoResultMessage
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      transactionResult <- ProtoConverter
        .required("verdict", verdictPO)
        .flatMap(Verdict.fromProtoV2)
      rootHash <- RootHash.fromProtoPrimitive(rootHashP)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
    } yield TransactionResultMessage(requestId, transactionResult, rootHash, domainId, None)(
      protocolVersionRepresentativeFor(ProtoVersion(2)),
      Some(bytes),
    )
  }

  private def fromProtoV3(
      _context: (HashOps, ProtocolVersion),
      protoResultMessage: v3.TransactionResultMessage,
  )(
      bytes: ByteString
  ): ParsingResult[TransactionResultMessage] = {
    val v3.TransactionResultMessage(requestIdPO, verdictPO, rootHashP, domainIdP) =
      protoResultMessage
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      transactionResult <- ProtoConverter
        .required("verdict", verdictPO)
        .flatMap(Verdict.fromProtoV3)
      rootHash <- RootHash.fromProtoPrimitive(rootHashP)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
    } yield TransactionResultMessage(requestId, transactionResult, rootHash, domainId, None)(
      protocolVersionRepresentativeFor(ProtoVersion(3)),
      Some(bytes),
    )
  }

  implicit val transactionResultMessageCast: SignedMessageContentCast[TransactionResultMessage] =
    SignedMessageContentCast.create[TransactionResultMessage]("TransactionResultMessage") {
      case m: TransactionResultMessage => Some(m)
      case _ => None
    }
}
