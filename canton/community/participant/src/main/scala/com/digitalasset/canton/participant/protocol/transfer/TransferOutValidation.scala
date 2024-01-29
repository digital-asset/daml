// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.FullTransferOutTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.protocol.LfTemplateId
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion

import scala.concurrent.ExecutionContext

/** Checks that need to be performed as part of phase 3 of the transfer-out request processing
  */
private[transfer] final case class TransferOutValidation(
    request: FullTransferOutTree,
    expectedStakeholders: Set[LfPartyId],
    expectedTemplateId: LfTemplateId,
    sourceProtocolVersion: SourceProtocolVersion,
    sourceTopology: TopologySnapshot,
    targetTopology: Option[TopologySnapshot],
    recipients: Recipients,
) {

  private def checkStakeholders(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] =
    condUnitET(
      request.stakeholders == expectedStakeholders,
      StakeholdersMismatch(
        None,
        declaredViewStakeholders = request.stakeholders,
        declaredContractStakeholders = None,
        expectedStakeholders = Right(expectedStakeholders),
      ),
    )

  private def checkParticipants(logger: TracedLogger)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] =
    targetTopology match {
      case Some(targetTopology) =>
        TransferOutValidationTransferringParticipant(
          request,
          expectedStakeholders,
          expectedTemplateId,
          sourceProtocolVersion,
          sourceTopology,
          targetTopology,
          recipients,
          logger,
        )
      case None =>
        TransferOutValidationNonTransferringParticipant(request, sourceTopology, logger)
    }
}

private[transfer] object TransferOutValidation {

  def apply(
      request: FullTransferOutTree,
      expectedStakeholders: Set[LfPartyId],
      expectedTemplateId: LfTemplateId,
      sourceProtocolVersion: SourceProtocolVersion,
      sourceTopology: TopologySnapshot,
      targetTopology: Option[TopologySnapshot],
      recipients: Recipients,
      logger: TracedLogger,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] = {

    val validation = TransferOutValidation(
      request,
      expectedStakeholders,
      expectedTemplateId,
      sourceProtocolVersion,
      sourceTopology,
      targetTopology,
      recipients,
    )

    for {
      _ <- validation.checkStakeholders
      _ <- validation.checkParticipants(logger)
      _ <- PVSourceDestinationDomainsAreCompatible(
        sourceProtocolVersion,
        request.targetDomainPV,
        request.contractId,
      )
    } yield ()
  }

}
