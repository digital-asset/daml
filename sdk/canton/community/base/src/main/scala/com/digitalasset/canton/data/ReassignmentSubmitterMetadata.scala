// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, UniqueIdentifier}

/** Information about the submitters of the transaction in the case of a reassignment.
  * This data structure is quite similar to [[com.digitalasset.canton.data.SubmitterMetadata]]
  * but differ on a small number of fields.
  */
final case class ReassignmentSubmitterMetadata(
    submitter: LfPartyId,
    submittingParticipant: ParticipantId,
    commandId: LedgerCommandId,
    submissionId: Option[LedgerSubmissionId],
    applicationId: LedgerApplicationId,
    workflowId: Option[LfWorkflowId],
) extends PrettyPrinting
    with HasSubmissionTrackerData {

  override def submissionTrackerData: Option[SubmissionTrackerData] = None

  def toProtoV30: v30.ReassignmentSubmitterMetadata =
    v30.ReassignmentSubmitterMetadata(
      submitter = submitter,
      submittingParticipantUid = submittingParticipant.uid.toProtoPrimitive,
      commandId = commandId,
      submissionId = submissionId.getOrElse(""),
      applicationId = applicationId,
      workflowId = workflowId.getOrElse(""),
    )

  override protected def pretty: Pretty[ReassignmentSubmitterMetadata] = prettyOfClass(
    param("submitter", _.submitter),
    param("submitting participant", _.submittingParticipant),
    param("command id", _.commandId),
    paramIfDefined("submission id", _.submissionId),
    param("application id", _.applicationId),
    param("workflow id", _.workflowId),
  )

  def submittingAdminParty: LfPartyId = submittingParticipant.adminParty.toLf
}

object ReassignmentSubmitterMetadata {
  def fromProtoV30(
      reassignmentSubmitterMetadataP: v30.ReassignmentSubmitterMetadata
  ): ParsingResult[ReassignmentSubmitterMetadata] = {
    val v30.ReassignmentSubmitterMetadata(
      submitterP,
      submittingParticipantP,
      commandIdP,
      submissionIdP,
      applicationIdP,
      workflowIdP,
    ) = reassignmentSubmitterMetadataP

    for {
      submitter <- ProtoConverter.parseLfPartyId(submitterP, "submitter")
      submittingParticipant <-
        UniqueIdentifier
          .fromProtoPrimitive(submittingParticipantP, "submitting_participant_uid")
          .map(ParticipantId(_))
      commandId <- ProtoConverter.parseCommandId(commandIdP)
      submissionId <- ProtoConverter.parseLFSubmissionIdO(submissionIdP)
      applicationId <- ProtoConverter.parseLFApplicationId(applicationIdP)
      workflowId <- ProtoConverter.parseLFWorkflowIdO(workflowIdP)
    } yield ReassignmentSubmitterMetadata(
      submitter,
      submittingParticipant,
      commandId,
      submissionId,
      applicationId,
      workflowId,
    )
  }
}
