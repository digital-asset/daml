// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services

import com.daml.ledger.api.v2.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForReassignmentRequest,
  SubmitAndWaitForReassignmentResponse,
}
import com.daml.ledger.api.v2.command_submission_service.*
import com.daml.ledger.api.v2.reassignment_commands.{
  ReassignmentCommand,
  ReassignmentCommands,
  UnassignCommand,
}
import com.digitalasset.canton.integration.tests.ledgerapi.auth.ServiceCallWithMainActorAuthTests

import java.util.UUID
import scala.concurrent.Future

trait SubmitDummyReassignment extends TestCommands { self: ServiceCallWithMainActorAuthTests =>

  private def dummyReassignmentCommands(
      party: String,
      userId: String,
  ): ReassignmentCommands =
    ReassignmentCommands(
      workflowId = "",
      userId = userId,
      commandId = UUID.randomUUID().toString,
      submitter = party,
      submissionId = UUID.randomUUID().toString,
      commands = Seq(
        ReassignmentCommand(
          command = ReassignmentCommand.Command.UnassignCommand(
            value = UnassignCommand(
              contractId =
                "00c1a1f804310d6808c83efae63e5ca9a910bca65337688f5b4124cf50bfacd709ca111220df56c89a6513a52a08ce13c62bebd8c888d87017912d9a2a2a213fab2deab8f6",
              source =
                "synchronizer1::12209bffe879cda98fa69f7bab97e002705cdb927b11d53c41f0b78b4c07a72a7a7a",
              target =
                "synchronizer2::12209bffe879cda98fa69f7bab97e002705cdb927b11d53c41f0b78b4c07a72a7a7b",
            )
          )
        )
      ),
    )

  protected def dummySubmitReassignmentRequest(
      party: String,
      userId: String,
  ): SubmitReassignmentRequest =
    SubmitReassignmentRequest(
      reassignmentCommands = Some(
        dummyReassignmentCommands(party, userId)
      )
    )

  protected def dummySubmitAndWaitForReassignmentRequest(
      party: String,
      userId: String,
  ): SubmitAndWaitForReassignmentRequest =
    SubmitAndWaitForReassignmentRequest(
      reassignmentCommands = Some(
        dummyReassignmentCommands(party, userId)
      ),
      eventFormat = None,
    )

  protected def submitReassignment(
      token: Option[String],
      party: String,
      userId: String,
  ): Future[SubmitReassignmentResponse] =
    stub(CommandSubmissionServiceGrpc.stub(channel), token)
      .submitReassignment(dummySubmitReassignmentRequest(party, userId))

  protected def submitAndWaitForReassignment(
      token: Option[String],
      party: String,
      userId: String,
  ): Future[SubmitAndWaitForReassignmentResponse] =
    stub(CommandServiceGrpc.stub(channel), token)
      .submitAndWaitForReassignment(dummySubmitAndWaitForReassignmentRequest(party, userId))

}
