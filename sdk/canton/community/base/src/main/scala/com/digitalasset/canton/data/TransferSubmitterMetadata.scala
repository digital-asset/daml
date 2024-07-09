// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, UniqueIdentifier}

/** Information about the submitters of the transaction in the case of a Transfer.
  * This data structure is quite similar to [[com.digitalasset.canton.data.SubmitterMetadata]]
  * but differ on a small number of fields.
  */
final case class TransferSubmitterMetadata(
    submitter: LfPartyId,
    submittingParticipant: ParticipantId,
    commandId: LedgerCommandId,
    submissionId: Option[LedgerSubmissionId],
    applicationId: LedgerApplicationId,
    workflowId: Option[LfWorkflowId],
) extends PrettyPrinting {

  def toProtoV30: v30.TransferSubmitterMetadata =
    v30.TransferSubmitterMetadata(
      submitter = submitter,
      submittingParticipantUid = submittingParticipant.uid.toProtoPrimitive,
      commandId = commandId,
      submissionId = submissionId.getOrElse(""),
      applicationId = applicationId,
      workflowId = workflowId.getOrElse(""),
    )

  override def pretty: Pretty[TransferSubmitterMetadata] = prettyOfClass(
    param("submitter", _.submitter),
    param("submitting participant", _.submittingParticipant),
    param("command id", _.commandId),
    paramIfDefined("submission id", _.submissionId),
    param("application id", _.applicationId),
    param("workflow id", _.workflowId),
  )
}

object TransferSubmitterMetadata {
  def fromProtoV30(
      transferSubmitterMetadataP: v30.TransferSubmitterMetadata
  ): ParsingResult[TransferSubmitterMetadata] = {
    val v30.TransferSubmitterMetadata(
      submitterP,
      submittingParticipantP,
      commandIdP,
      submissionIdP,
      applicationIdP,
      workflowIdP,
    ) = transferSubmitterMetadataP

    for {
      submitter <- ProtoConverter.parseLfPartyId(submitterP)
      submittingParticipant <-
        UniqueIdentifier
          .fromProtoPrimitive(submittingParticipantP, "submitting_participant_uid")
          .map(ParticipantId(_))
      commandId <- ProtoConverter.parseCommandId(commandIdP)
      submissionId <- ProtoConverter.parseLFSubmissionIdO(submissionIdP)
      applicationId <- ProtoConverter.parseLFApplicationId(applicationIdP)
      workflowId <- ProtoConverter.parseLFWorkflowIdO(workflowIdP)
    } yield TransferSubmitterMetadata(
      submitter,
      submittingParticipant,
      commandId,
      submissionId,
      applicationId,
      workflowId,
    )
  }
}
