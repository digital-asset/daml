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
final case class TransferInMediatorMessage(tree: TransferInViewTree) extends MediatorRequest {

  require(tree.commonData.isFullyUnblinded, "The transfer-in common data must be unblinded")
  require(tree.view.isBlinded, "The transfer-in view must be blinded")

  private[this] val commonData = tree.commonData.tryUnwrap

  // Align the protocol version with the common data's protocol version
  lazy val protocolVersion: TargetProtocolVersion = commonData.targetProtocolVersion

  override lazy val representativeProtocolVersion
      : RepresentativeProtocolVersion[TransferInMediatorMessage.type] =
    TransferInMediatorMessage.protocolVersionRepresentativeFor(protocolVersion.v)

  override def domainId: DomainId = commonData.targetDomain.unwrap

  override def mediator: MediatorRef = commonData.targetMediator

  override def requestUuid: UUID = commonData.uuid

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

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.TransferInMediatorMessage(toProtoV30)

  def toProtoV30: v30.TransferInMediatorMessage =
    v30.TransferInMediatorMessage(tree = Some(tree.toProtoV30))

  override def rootHash: RootHash = tree.rootHash

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
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.TransferInMediatorMessage)(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30.toByteString,
    )
  )

  def fromProtoV30(context: (HashOps, ProtocolVersion))(
      transferInMediatorMessageP: v30.TransferInMediatorMessage
  ): ParsingResult[TransferInMediatorMessage] = {
    val v30.TransferInMediatorMessage(treePO) =
      transferInMediatorMessageP
    for {
      tree <- ProtoConverter
        .required("TransferInMediatorMessage.tree", treePO)
        .flatMap(TransferInViewTree.fromProtoV30(context, _))
      _ <- EitherUtil.condUnitE(
        tree.commonData.isFullyUnblinded,
        OtherError(s"Transfer-in common data is blinded in request ${tree.rootHash}"),
      )
      _ <- EitherUtil.condUnitE(
        tree.view.isBlinded,
        OtherError(s"Transfer-in view data is not blinded in request ${tree.rootHash}"),
      )
    } yield TransferInMediatorMessage(tree)
  }

  override def name: String = "TransferInMediatorMessage"
}
