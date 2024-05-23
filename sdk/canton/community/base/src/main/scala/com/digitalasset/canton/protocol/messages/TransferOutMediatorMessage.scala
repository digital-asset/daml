// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{HashOps, Signature}
import com.digitalasset.canton.data.{Informee, TransferOutViewTree, ViewPosition, ViewType}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion
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
    tree: TransferOutViewTree,
    override val submittingParticipantSignature: Signature,
) extends MediatorConfirmationRequest
    with UnsignedProtocolMessage {
  require(tree.commonData.isFullyUnblinded, "The transfer-out common data must be unblinded")
  require(tree.view.isBlinded, "The transfer-out view must be blinded")

  private[this] val commonData = tree.commonData.tryUnwrap

  override def submittingParticipant: ParticipantId = tree.submittingParticipant

  val protocolVersion = commonData.sourceProtocolVersion

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[TransferOutMediatorMessage.type] =
    TransferOutMediatorMessage.protocolVersionRepresentativeFor(protocolVersion.v)

  override def domainId: DomainId = commonData.sourceDomain.unwrap

  override def mediator: MediatorGroupRecipient = commonData.sourceMediator

  override def requestUuid: UUID = commonData.uuid

  override def informeesAndThresholdByViewPosition
      : Map[ViewPosition, (Set[Informee], NonNegativeInt)] = {
    val confirmingParties = commonData.confirmingParties
    val threshold = NonNegativeInt.tryCreate(confirmingParties.size)
    Map(tree.viewPosition -> ((confirmingParties, threshold)))
  }

  override def minimumThreshold(informees: Set[Informee]): NonNegativeInt = NonNegativeInt.one

  def toProtoV30: v30.TransferOutMediatorMessage =
    v30.TransferOutMediatorMessage(
      tree = Some(tree.toProtoV30),
      submittingParticipantSignature = Some(submittingParticipantSignature.toProtoV30),
    )

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.TransferOutMediatorMessage(toProtoV30)

  override def rootHash: RootHash = tree.rootHash

  override def viewType: ViewType = ViewType.TransferOutViewType

  override def pretty: Pretty[TransferOutMediatorMessage] = prettyOfClass(unnamedParam(_.tree))

  @transient override protected lazy val companionObj: TransferOutMediatorMessage.type =
    TransferOutMediatorMessage

  override def informeesArePublic: Boolean = true
}

object TransferOutMediatorMessage
    extends HasProtocolVersionedWithContextCompanion[
      TransferOutMediatorMessage,
      (HashOps, SourceProtocolVersion),
    ] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(
      v30.TransferOutMediatorMessage
    )(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30.toByteString,
    )
  )

  def fromProtoV30(context: (HashOps, SourceProtocolVersion))(
      transferOutMediatorMessageP: v30.TransferOutMediatorMessage
  ): ParsingResult[TransferOutMediatorMessage] = {
    val v30.TransferOutMediatorMessage(treePO, submittingParticipantSignaturePO) =
      transferOutMediatorMessageP
    for {
      tree <- ProtoConverter
        .required("TransferOutMediatorMessage.tree", treePO)
        .flatMap(TransferOutViewTree.fromProtoV30(context))
      _ <- EitherUtil.condUnitE(
        tree.commonData.isFullyUnblinded,
        OtherError(s"Transfer-out common data is blinded in request ${tree.rootHash}"),
      )
      _ <- EitherUtil.condUnitE(
        tree.view.isBlinded,
        OtherError(s"Transfer-out view data is not blinded in request ${tree.rootHash}"),
      )
      submittingParticipantSignature <- ProtoConverter
        .required(
          "TransferOutMediatorMessage.submittingParticipantSignature",
          submittingParticipantSignaturePO,
        )
        .flatMap(Signature.fromProtoV30)
    } yield TransferOutMediatorMessage(tree, submittingParticipantSignature)
  }

  override def name: String = "TransferOutMediatorMessage"
}
