// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    with UnsignedProtocolMessage {
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

  def toProtoV1: v1.TransferOutMediatorMessage =
    v1.TransferOutMediatorMessage(tree = Some(tree.toProtoV1))

  override def toProtoSomeEnvelopeContentV4: v4.EnvelopeContent.SomeEnvelopeContent =
    v4.EnvelopeContent.SomeEnvelopeContent.TransferOutMediatorMessage(toProtoV1)

  override def rootHash: Option[RootHash] = Some(tree.rootHash)

  override def viewType: ViewType = ViewType.TransferOutViewType

  override def pretty: Pretty[TransferOutMediatorMessage] = prettyOfClass(unnamedParam(_.tree))

  @transient override protected lazy val companionObj: TransferOutMediatorMessage.type =
    TransferOutMediatorMessage
}

object TransferOutMediatorMessage
    extends HasProtocolVersionedWithContextCompanion[TransferOutMediatorMessage, HashOps] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v1.TransferOutMediatorMessage
    )(
      supportedProtoVersion(_)((hashOps, proto) => fromProtoV1(hashOps)(proto)),
      _.toProtoV1.toByteString,
    )
  )

  def fromProtoV1(hashOps: HashOps)(
      transferOutMediatorMessageP: v1.TransferOutMediatorMessage
  ): ParsingResult[TransferOutMediatorMessage] =
    for {
      tree <- ProtoConverter
        .required("TransferOutMediatorMessage.tree", transferOutMediatorMessageP.tree)
        .flatMap(TransferOutViewTree.fromProtoV1(hashOps))
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
