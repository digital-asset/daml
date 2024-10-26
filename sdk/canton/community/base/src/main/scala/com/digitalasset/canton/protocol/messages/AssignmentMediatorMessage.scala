// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{HashOps, Signature}
import com.digitalasset.canton.data.{
  AssignmentViewTree,
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
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

import java.util.UUID

/** Message sent to the mediator as part of an assignment request
  *
  * @param tree The assignment view tree blinded for the mediator
  * @throws java.lang.IllegalArgumentException if the common data is blinded or the view is not blinded
  */
final case class AssignmentMediatorMessage(
    tree: AssignmentViewTree,
    override val submittingParticipantSignature: Signature,
) extends MediatorConfirmationRequest {

  require(tree.commonData.isFullyUnblinded, "The assignment common data must be unblinded")
  require(tree.view.isBlinded, "The assignment view must be blinded")

  override def submittingParticipant: ParticipantId = tree.submittingParticipant

  private[this] val commonData = tree.commonData.tryUnwrap

  // Align the protocol version with the common data's protocol version
  lazy val protocolVersion: Target[ProtocolVersion] = commonData.targetProtocolVersion

  override lazy val representativeProtocolVersion
      : RepresentativeProtocolVersion[AssignmentMediatorMessage.type] =
    AssignmentMediatorMessage.protocolVersionRepresentativeFor(protocolVersion.unwrap)

  override def domainId: DomainId = commonData.targetDomain.unwrap

  override def mediator: MediatorGroupRecipient = commonData.targetMediatorGroup

  override def requestUuid: UUID = commonData.uuid

  override def informeesAndConfirmationParamsByViewPosition
      : Map[ViewPosition, ViewConfirmationParameters] = {
    val confirmingParties = commonData.confirmingParties
    val viewThreshold = NonNegativeInt.tryCreate(confirmingParties.size)

    Map(
      tree.viewPosition -> ViewConfirmationParameters.createOnlyWithConfirmers(
        confirmingParties,
        viewThreshold,
      )
    )
  }

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.AssignmentMediatorMessage(toProtoV30)

  def toProtoV30: v30.AssignmentMediatorMessage =
    v30.AssignmentMediatorMessage(
      tree = Some(tree.toProtoV30),
      submittingParticipantSignature = Some(submittingParticipantSignature.toProtoV30),
    )

  override def rootHash: RootHash = tree.rootHash

  override def viewType: ViewType = ViewType.AssignmentViewType

  override def pretty: Pretty[AssignmentMediatorMessage] = prettyOfClass(unnamedParam(_.tree))

  @transient override protected lazy val companionObj: AssignmentMediatorMessage.type =
    AssignmentMediatorMessage

  override def informeesArePublic: Boolean = true
}

object AssignmentMediatorMessage
    extends HasProtocolVersionedWithContextCompanion[
      AssignmentMediatorMessage,
      (HashOps, Target[ProtocolVersion]),
    ] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(v30.AssignmentMediatorMessage)(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30.toByteString,
    )
  )

  def fromProtoV30(context: (HashOps, Target[ProtocolVersion]))(
      assignmentMediatorMessageP: v30.AssignmentMediatorMessage
  ): ParsingResult[AssignmentMediatorMessage] = {
    val v30.AssignmentMediatorMessage(treePO, submittingParticipantSignaturePO) =
      assignmentMediatorMessageP
    for {
      tree <- ProtoConverter
        .required("AssignmentMediatorMessage.tree", treePO)
        .flatMap(AssignmentViewTree.fromProtoV30(context))
      _ <- Either.cond(
        tree.commonData.isFullyUnblinded,
        (),
        OtherError(s"Assignment common data is blinded in request ${tree.rootHash}"),
      )
      _ <- Either.cond(
        tree.view.isBlinded,
        (),
        OtherError(s"Assignment view data is not blinded in request ${tree.rootHash}"),
      )
      submittingParticipantSignature <- ProtoConverter
        .required(
          "AssignmentMediatorMessage.submittingParticipantSignature",
          submittingParticipantSignaturePO,
        )
        .flatMap(Signature.fromProtoV30)
    } yield AssignmentMediatorMessage(tree, submittingParticipantSignature)
  }

  override def name: String = "AssignmentMediatorMessage"
}
