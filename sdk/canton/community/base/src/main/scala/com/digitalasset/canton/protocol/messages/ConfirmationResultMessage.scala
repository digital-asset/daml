// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, RootHash, v30}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** Result message that the mediator sends to all informees of a request with its verdict.
  *
  * @param psid
  *   the synchronizer on which the request is running
  * @param viewType
  *   determines which processor (transaction / reassignment) must process this message
  * @param requestId
  *   unique identifier of the confirmation request
  * @param rootHash
  *   hash over the contents of the request
  * @param verdict
  *   the finalized verdict on the request
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class ConfirmationResultMessage private (
    override val psid: PhysicalSynchronizerId,
    viewType: ViewType,
    override val requestId: RequestId,
    rootHash: RootHash,
    verdict: Verdict,
)(
    override val deserializedFrom: Option[ByteString]
) extends ProtocolVersionedMemoizedEvidence
    with HasPhysicalSynchronizerId
    with HasRequestId
    with SignedProtocolMessageContent
    with HasProtocolVersionedWrapper[ConfirmationResultMessage]
    with PrettyPrinting {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    ConfirmationResultMessage.type
  ] = ConfirmationResultMessage.protocolVersionRepresentativeFor(psid.protocolVersion)

  override def signingTimestamp: Option[CantonTimestamp] = Some(requestId.unwrap)

  def copy(
      synchronizerId: PhysicalSynchronizerId = this.psid,
      viewType: ViewType = this.viewType,
      requestId: RequestId = this.requestId,
      rootHash: RootHash = this.rootHash,
      verdict: Verdict = this.verdict,
  ): ConfirmationResultMessage =
    ConfirmationResultMessage(synchronizerId, viewType, requestId, rootHash, verdict)(
      None
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  @transient override protected lazy val companionObj: ConfirmationResultMessage.type =
    ConfirmationResultMessage

  protected def toProtoV30: v30.ConfirmationResultMessage =
    v30.ConfirmationResultMessage(
      physicalSynchronizerId = psid.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
      requestId = requestId.toProtoPrimitive,
      rootHash = rootHash.toProtoPrimitive,
      verdict = Some(verdict.toProtoV30),
    )

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.ConfirmationResult(
      getCryptographicEvidence
    )

  @VisibleForTesting
  override def pretty: Pretty[ConfirmationResultMessage] =
    prettyOfClass(
      param("psid", _.psid),
      param("viewType", _.viewType),
      param("requestId", _.requestId.unwrap),
      param("rootHash", _.rootHash),
      param("verdict", _.verdict),
    )
}

object ConfirmationResultMessage
    extends VersioningCompanionMemoization[
      ConfirmationResultMessage,
    ] {
  override val name: String = "ConfirmationResultMessage"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(
      v30.ConfirmationResultMessage
    )(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def create(
      synchronizerId: PhysicalSynchronizerId,
      viewType: ViewType,
      requestId: RequestId,
      rootHash: RootHash,
      verdict: Verdict,
  ): ConfirmationResultMessage =
    ConfirmationResultMessage(synchronizerId, viewType, requestId, rootHash, verdict)(None)

  private def fromProtoV30(protoResultMessage: v30.ConfirmationResultMessage)(
      bytes: ByteString
  ): ParsingResult[ConfirmationResultMessage] = {
    val v30.ConfirmationResultMessage(
      synchronizerIdP,
      viewTypeP,
      requestIdP,
      rootHashP,
      verdictPO,
    ) = protoResultMessage

    for {
      synchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(
        synchronizerIdP,
        "physical_synchronizer_id",
      )
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      requestId <- RequestId.fromProtoPrimitive(requestIdP)
      rootHash <- RootHash.fromProtoPrimitive(rootHashP)
      verdict <- ProtoConverter.parseRequired(Verdict.fromProtoV30, "verdict", verdictPO)
    } yield ConfirmationResultMessage(
      synchronizerId,
      viewType,
      requestId,
      rootHash,
      verdict,
    )(Some(bytes))
  }

  implicit val confirmationResultMessageCast: SignedMessageContentCast[ConfirmationResultMessage] =
    SignedMessageContentCast.create[ConfirmationResultMessage]("ConfirmationResultMessage") {
      case m: ConfirmationResultMessage => Some(m)
      case _ => None
    }
}
