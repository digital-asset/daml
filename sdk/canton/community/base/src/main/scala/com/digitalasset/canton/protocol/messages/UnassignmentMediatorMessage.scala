// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{HashOps, Signature}
import com.digitalasset.canton.data.{
  UnassignmentViewTree,
  ViewConfirmationParameters,
  ViewPosition,
  ViewType,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

import java.util.UUID

/** Message sent to the mediator as part of an unassignment request
  *
  * @param tree The unassignment view tree blinded for the mediator
  * @throws java.lang.IllegalArgumentException if the common data is blinded or the view is not blinded
  */
final case class UnassignmentMediatorMessage(
    tree: UnassignmentViewTree,
    override val submittingParticipantSignature: Signature,
) extends MediatorConfirmationRequest
    with UnsignedProtocolMessage {
  require(tree.commonData.isFullyUnblinded, "The unassignment common data must be unblinded")
  require(tree.view.isBlinded, "The unassignment view must be blinded")

  private[this] val commonData = tree.commonData.tryUnwrap

  override def submittingParticipant: ParticipantId = tree.submittingParticipant

  val protocolVersion: Source[ProtocolVersion] = commonData.sourceProtocolVersion

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[UnassignmentMediatorMessage.type] =
    UnassignmentMediatorMessage.protocolVersionRepresentativeFor(protocolVersion.unwrap)

  override def domainId: DomainId = commonData.sourceDomain.unwrap

  override def mediator: MediatorGroupRecipient = commonData.sourceMediatorGroup

  override def requestUuid: UUID = commonData.uuid

  override def informeesAndConfirmationParamsByViewPosition
      : Map[ViewPosition, ViewConfirmationParameters] = {
    val confirmingParties = commonData.confirmingParties
    val threshold = NonNegativeInt.tryCreate(confirmingParties.size)
    Map(
      tree.viewPosition -> ViewConfirmationParameters.createOnlyWithConfirmers(
        confirmingParties,
        threshold,
      )
    )
  }

  def toProtoV30: v30.UnassignmentMediatorMessage =
    v30.UnassignmentMediatorMessage(
      tree = Some(tree.toProtoV30),
      submittingParticipantSignature = Some(submittingParticipantSignature.toProtoV30),
    )

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.UnassignmentMediatorMessage(toProtoV30)

  override def rootHash: RootHash = tree.rootHash

  override def viewType: ViewType = ViewType.UnassignmentViewType

  override def pretty: Pretty[UnassignmentMediatorMessage] = prettyOfClass(unnamedParam(_.tree))

  @transient override protected lazy val companionObj: UnassignmentMediatorMessage.type =
    UnassignmentMediatorMessage

  override def informeesArePublic: Boolean = true
}

object UnassignmentMediatorMessage
    extends HasProtocolVersionedWithContextCompanion[
      UnassignmentMediatorMessage,
      (HashOps, Source[ProtocolVersion]),
    ] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(
      v30.UnassignmentMediatorMessage
    )(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30.toByteString,
    )
  )

  def fromProtoV30(context: (HashOps, Source[ProtocolVersion]))(
      unassignmentMediatorMessageP: v30.UnassignmentMediatorMessage
  ): ParsingResult[UnassignmentMediatorMessage] = {
    val v30.UnassignmentMediatorMessage(treePO, submittingParticipantSignaturePO) =
      unassignmentMediatorMessageP
    for {
      tree <- ProtoConverter
        .required("UnassignmentMediatorMessage.tree", treePO)
        .flatMap(UnassignmentViewTree.fromProtoV30(context))
      _ <- Either.cond(
        tree.commonData.isFullyUnblinded,
        (),
        OtherError(s"Unassignment common data is blinded in request ${tree.rootHash}"),
      )
      _ <- Either.cond(
        tree.view.isBlinded,
        (),
        OtherError(s"Unassignment view data is not blinded in request ${tree.rootHash}"),
      )
      submittingParticipantSignature <- ProtoConverter
        .required(
          "UnassignmentMediatorMessage.submittingParticipantSignature",
          submittingParticipantSignaturePO,
        )
        .flatMap(Signature.fromProtoV30)
    } yield UnassignmentMediatorMessage(tree, submittingParticipantSignature)
  }

  override def name: String = "UnassignmentMediatorMessage"
}
