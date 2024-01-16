// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.{Informee, TransferInViewTree, ViewPosition, ViewType}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, MediatorRef}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.Transfer.TargetProtocolVersion
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

import java.util.UUID

/** Message sent to the mediator as part of a transfer-in request
  *
  * @param tree The transfer-in view tree blinded for the mediator
  * @throws java.lang.IllegalArgumentException if the common data is blinded or the view is not blinded
  */
final case class TransferInMediatorMessage(tree: TransferInViewTree)
    extends MediatorRequest
    with ProtocolMessageV0
    with ProtocolMessageV1
    with ProtocolMessageV2
    with ProtocolMessageV3
    with UnsignedProtocolMessageV4 {

  require(tree.commonData.isFullyUnblinded, "The transfer-in common data must be unblinded")
  require(tree.view.isBlinded, "The transfer-out view must be blinded")

  private[this] val commonData = tree.commonData.tryUnwrap

  // Align the protocol version with the common data's protocol version
  lazy val protocolVersion: TargetProtocolVersion = commonData.targetProtocolVersion

  override lazy val representativeProtocolVersion
      : RepresentativeProtocolVersion[TransferInMediatorMessage.type] =
    TransferInMediatorMessage.protocolVersionRepresentativeFor(protocolVersion.v)

  override def domainId: DomainId = commonData.targetDomain.unwrap

  override def mediator: MediatorRef = commonData.targetMediator

  override def requestUuid: UUID = commonData.uuid

  override def informeesAndThresholdByViewHash: Map[ViewHash, (Set[Informee], NonNegativeInt)] = {
    val confirmingParties = commonData.confirmingParties
    val threshold = NonNegativeInt.tryCreate(confirmingParties.size)
    Map(tree.viewHash -> ((confirmingParties, threshold)))
  }

  override def informeesAndThresholdByViewPosition
      : Map[ViewPosition, (Set[Informee], NonNegativeInt)] = {
    val confirmingParties = commonData.confirmingParties
    val threshold = NonNegativeInt.tryCreate(confirmingParties.size)
    Map(tree.viewPosition -> ((confirmingParties, threshold)))
  }

  override def minimumThreshold(informees: Set[Informee]): NonNegativeInt = NonNegativeInt.one

  override def createMediatorResult(
      requestId: RequestId,
      verdict: Verdict,
      recipientParties: Set[LfPartyId],
  ): MediatorResult with SignedProtocolMessageContent = {
    val informees = commonData.stakeholders
    require(
      recipientParties.subsetOf(informees),
      "Recipient parties of the transfer-in result must be stakeholders.",
    )
    TransferResult.create(
      requestId,
      informees,
      commonData.targetDomain,
      verdict,
      protocolVersion.v,
    )
  }

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(
      someEnvelopeContent =
        v0.EnvelopeContent.SomeEnvelopeContent.TransferInMediatorMessage(toProtoV0)
    )

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(
      someEnvelopeContent =
        v1.EnvelopeContent.SomeEnvelopeContent.TransferInMediatorMessage(toProtoV1)
    )

  override def toProtoEnvelopeContentV2: v2.EnvelopeContent =
    v2.EnvelopeContent(
      someEnvelopeContent =
        v2.EnvelopeContent.SomeEnvelopeContent.TransferInMediatorMessage(toProtoV1)
    )

  override def toProtoEnvelopeContentV3: v3.EnvelopeContent =
    v3.EnvelopeContent(
      someEnvelopeContent =
        v3.EnvelopeContent.SomeEnvelopeContent.TransferInMediatorMessage(toProtoV1)
    )

  override def toProtoSomeEnvelopeContentV4: v4.EnvelopeContent.SomeEnvelopeContent =
    v4.EnvelopeContent.SomeEnvelopeContent.TransferInMediatorMessage(toProtoV1)

  def toProtoV0: v0.TransferInMediatorMessage =
    v0.TransferInMediatorMessage(tree = Some(tree.toProtoV0))

  def toProtoV1: v1.TransferInMediatorMessage =
    v1.TransferInMediatorMessage(tree = Some(tree.toProtoV1))

  override def rootHash: Option[RootHash] = Some(tree.rootHash)

  override def viewType: ViewType = ViewType.TransferInViewType

  override def pretty: Pretty[TransferInMediatorMessage] = prettyOfClass(unnamedParam(_.tree))

  @transient override protected lazy val companionObj: TransferInMediatorMessage.type =
    TransferInMediatorMessage
}

object TransferInMediatorMessage
    extends HasProtocolVersionedWithContextCompanion[
      TransferInMediatorMessage,
      (HashOps, ProtocolVersion),
    ] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransferInMediatorMessage)(
      supportedProtoVersion(_)((context, proto) => fromProtoV0(context)(proto)),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransferInMediatorMessage)(
      supportedProtoVersion(_)((context, proto) => fromProtoV1(context)(proto)),
      _.toProtoV1.toByteString,
    ),
  )

  def fromProtoV0(context: (HashOps, ProtocolVersion))(
      transferInMediatorMessageP: v0.TransferInMediatorMessage
  ): ParsingResult[TransferInMediatorMessage] =
    for {
      tree <- ProtoConverter
        .required("TransferInMediatorMessage.tree", transferInMediatorMessageP.tree)
        .flatMap(TransferInViewTree.fromProtoV0(context, _))
      _ <- EitherUtil.condUnitE(
        tree.commonData.isFullyUnblinded,
        OtherError(s"Transfer-in common data is blinded in request ${tree.rootHash}"),
      )
      _ <- EitherUtil.condUnitE(
        tree.view.isBlinded,
        OtherError(s"Transfer-in view data is not blinded in request ${tree.rootHash}"),
      )
    } yield TransferInMediatorMessage(tree)

  def fromProtoV1(context: (HashOps, ProtocolVersion))(
      transferInMediatorMessageP: v1.TransferInMediatorMessage
  ): ParsingResult[TransferInMediatorMessage] =
    for {
      tree <- ProtoConverter
        .required("TransferInMediatorMessage.tree", transferInMediatorMessageP.tree)
        .flatMap(TransferInViewTree.fromProtoV1(context, _))
      _ <- EitherUtil.condUnitE(
        tree.commonData.isFullyUnblinded,
        OtherError(s"Transfer-in common data is blinded in request ${tree.rootHash}"),
      )
      _ <- EitherUtil.condUnitE(
        tree.view.isBlinded,
        OtherError(s"Transfer-in view data is not blinded in request ${tree.rootHash}"),
      )
    } yield TransferInMediatorMessage(tree)

  override def name: String = "TransferInMediatorMessage"
}
