// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.{Informee, TransferOutViewTree, ViewPosition, ViewType}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, MediatorRef}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

import java.util.UUID

/** Message sent to the mediator as part of a transfer-out request
  *
  * @param tree The transfer-out view tree blinded for the mediator
  * @throws java.lang.IllegalArgumentException if the common data is blinded or the view is not blinded
  */
final case class TransferOutMediatorMessage(
    tree: TransferOutViewTree
) extends MediatorRequest
    with ProtocolMessageV0
    with ProtocolMessageV1
    with ProtocolMessageV2
    with ProtocolMessageV3
    with UnsignedProtocolMessageV4 {
  require(tree.commonData.isFullyUnblinded, "The transfer-out common data must be unblinded")
  require(tree.view.isBlinded, "The transfer-out view must be blinded")

  private[this] val commonData = tree.commonData.tryUnwrap

  val protocolVersion = commonData.protocolVersion

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[TransferOutMediatorMessage.type] =
    TransferOutMediatorMessage.protocolVersionRepresentativeFor(protocolVersion.v)

  override def domainId: DomainId = commonData.sourceDomain.unwrap

  override def mediator: MediatorRef = commonData.sourceMediator

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
    val informees = commonData.stakeholders ++ commonData.adminParties
    require(
      recipientParties.subsetOf(informees),
      "Recipient parties of the transfer-out result are neither stakeholders nor admin parties",
    )
    TransferResult.create(
      requestId,
      informees,
      commonData.sourceDomain,
      verdict,
      protocolVersion.v,
    )
  }

  def toProtoV0: v0.TransferOutMediatorMessage =
    v0.TransferOutMediatorMessage(tree = Some(tree.toProtoV0))

  def toProtoV1: v1.TransferOutMediatorMessage =
    v1.TransferOutMediatorMessage(tree = Some(tree.toProtoV1))

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(v0.EnvelopeContent.SomeEnvelopeContent.TransferOutMediatorMessage(toProtoV0))

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(v1.EnvelopeContent.SomeEnvelopeContent.TransferOutMediatorMessage(toProtoV1))

  override def toProtoEnvelopeContentV2: v2.EnvelopeContent =
    v2.EnvelopeContent(v2.EnvelopeContent.SomeEnvelopeContent.TransferOutMediatorMessage(toProtoV1))

  override def toProtoEnvelopeContentV3: v3.EnvelopeContent =
    v3.EnvelopeContent(v3.EnvelopeContent.SomeEnvelopeContent.TransferOutMediatorMessage(toProtoV1))

  override def toProtoSomeEnvelopeContentV4: v4.EnvelopeContent.SomeEnvelopeContent =
    v4.EnvelopeContent.SomeEnvelopeContent.TransferOutMediatorMessage(toProtoV1)

  override def rootHash: Option[RootHash] = Some(tree.rootHash)

  override def viewType: ViewType = ViewType.TransferOutViewType

  override def pretty: Pretty[TransferOutMediatorMessage] = prettyOfClass(unnamedParam(_.tree))

  @transient override protected lazy val companionObj: TransferOutMediatorMessage.type =
    TransferOutMediatorMessage
}

object TransferOutMediatorMessage
    extends HasProtocolVersionedWithContextCompanion[
      TransferOutMediatorMessage,
      (HashOps, ProtocolVersion),
    ] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(
      v0.TransferOutMediatorMessage
    )(
      supportedProtoVersion(_)((context, proto) => fromProtoV0(context)(proto)),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(
      v1.TransferOutMediatorMessage
    )(
      supportedProtoVersion(_)((context, proto) => fromProtoV1(context)(proto)),
      _.toProtoV1.toByteString,
    ),
  )

  def fromProtoV0(context: (HashOps, ProtocolVersion))(
      transferOutMediatorMessageP: v0.TransferOutMediatorMessage
  ): ParsingResult[TransferOutMediatorMessage] =
    for {
      tree <- ProtoConverter
        .required("TransferOutMediatorMessage.tree", transferOutMediatorMessageP.tree)
        .flatMap(TransferOutViewTree.fromProtoV0(context))
      _ <- EitherUtil.condUnitE(
        tree.commonData.isFullyUnblinded,
        OtherError(s"Transfer-out common data is blinded in request ${tree.rootHash}"),
      )
      _ <- EitherUtil.condUnitE(
        tree.view.isBlinded,
        OtherError(s"Transfer-out view data is not blinded in request ${tree.rootHash}"),
      )
    } yield TransferOutMediatorMessage(tree)

  def fromProtoV1(context: (HashOps, ProtocolVersion))(
      transferOutMediatorMessageP: v1.TransferOutMediatorMessage
  ): ParsingResult[TransferOutMediatorMessage] =
    for {
      tree <- ProtoConverter
        .required("TransferOutMediatorMessage.tree", transferOutMediatorMessageP.tree)
        .flatMap(TransferOutViewTree.fromProtoV1(context))
      _ <- EitherUtil.condUnitE(
        tree.commonData.isFullyUnblinded,
        OtherError(s"Transfer-out common data is blinded in request ${tree.rootHash}"),
      )
      _ <- EitherUtil.condUnitE(
        tree.view.isBlinded,
        OtherError(s"Transfer-out view data is not blinded in request ${tree.rootHash}"),
      )
    } yield TransferOutMediatorMessage(tree)

  override def name: String = "TransferOutMediatorMessage"
}
