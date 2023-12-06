// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.FullTransferOutTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessorError.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.protocol.LfTemplateId
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion

import scala.concurrent.ExecutionContext

private[transfer] sealed abstract case class TransferOutValidationTransferringParticipant(
    request: FullTransferOutTree,
    expectedStakeholders: Set[LfPartyId],
    sourceProtocolVersion: SourceProtocolVersion,
    sourceTopology: TopologySnapshot,
    targetTopology: TopologySnapshot,
    recipients: Recipients,
) {

  private def checkAdminParties(
      expectedAdminParties: Set[LfPartyId]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] =
    condUnitET[FutureUnlessShutdown](
      request.adminParties == expectedAdminParties,
      AdminPartiesMismatch(
        contractId = request.contractId,
        expected = expectedAdminParties,
        declared = request.adminParties,
      ),
    )

  private def checkParticipants(
      expectedParticipants: Set[ParticipantId]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] = {
    val expectedRecipientsTree = Recipients.ofSet(expectedParticipants)
    condUnitET[FutureUnlessShutdown](
      expectedRecipientsTree.contains(recipients),
      RecipientsMismatch(
        contractId = request.contractId,
        expected = expectedRecipientsTree,
        declared = recipients,
      ),
    )
  }

  private def checkVetted(stakeholders: Set[LfPartyId], templateId: LfTemplateId)(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] =
    TransferKnownAndVetted(
      stakeholders,
      targetTopology,
      request.contractId,
      templateId.packageId,
      request.targetDomain,
    )
}

private[transfer] object TransferOutValidationTransferringParticipant {

  def apply(
      request: FullTransferOutTree,
      expectedStakeholders: Set[LfPartyId],
      expectedTemplateId: LfTemplateId,
      sourceProtocolVersion: SourceProtocolVersion,
      sourceTopology: TopologySnapshot,
      targetTopology: TopologySnapshot,
      recipients: Recipients,
      logger: TracedLogger,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] = {
    val validation = new TransferOutValidationTransferringParticipant(
      request,
      expectedStakeholders,
      sourceProtocolVersion,
      sourceTopology,
      targetTopology,
      recipients,
    ) {}
    for {
      adminPartiesAndParticipants <- AdminPartiesAndParticipants(
        request.contractId,
        request.submitter,
        expectedStakeholders,
        sourceTopology,
        targetTopology,
        logger,
      )
      _ <- validation.checkAdminParties(adminPartiesAndParticipants.adminParties)
      _ <- validation.checkParticipants(adminPartiesAndParticipants.participants)
      _ <- validation.checkVetted(expectedStakeholders, expectedTemplateId)
    } yield ()
  }

}
