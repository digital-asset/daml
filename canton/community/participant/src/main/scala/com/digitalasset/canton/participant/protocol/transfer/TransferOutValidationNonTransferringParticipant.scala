// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.{EitherT, Validated, ValidatedNec}
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.FullTransferOutTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessorError.{
  AdminPartyPermissionErrors,
  StakeholderHostingErrors,
  fromChain,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

private[transfer] sealed abstract case class TransferOutValidationNonTransferringParticipant(
    request: FullTransferOutTree,
    sourceTopology: TopologySnapshot,
) {

  private[this] def checkStakeholderHasTransferringParticipant(
      stakeholder: LfPartyId
  )(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[ValidatedNec[String, Unit]] =
    FutureUnlessShutdown.outcomeF(
      for (participants <- sourceTopology.activeParticipantsOf(stakeholder)) yield {
        val hasTransferringParticipant =
          participants.exists { case (participant, attributes) =>
            attributes.permission.canConfirm && request.adminParties.contains(
              participant.adminParty.toLf
            )
          }
        Validated.condNec(
          hasTransferringParticipant,
          (),
          s"Stakeholder $stakeholder has no transferring participant.",
        )
      }
    )

  private def stakeholdersHaveTransferringParticipant(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, List[Unit]] =
    EitherT(
      request.stakeholders.toList
        .parTraverse(checkStakeholderHasTransferringParticipant)
        .map(
          _.sequence.toEither
            .leftMap(fromChain(StakeholderHostingErrors))
        )
    )

  private def adminPartiesCanConfirm(logger: TracedLogger)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, List[Unit]] =
    EitherT(
      request.adminParties.toList
        .parTraverse(adminPartyCanConfirm(logger))
        .map(
          _.sequence.toEither
            .leftMap(fromChain(AdminPartyPermissionErrors))
        )
    )

  private[this] def adminPartyCanConfirm(logger: TracedLogger)(adminParty: LfPartyId)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[ValidatedNec[String, Unit]] =
    TransferOutValidationUtil
      .confirmingAdminParticipants(sourceTopology, adminParty, logger)
      .map { adminParticipants =>
        Validated.condNec(
          adminParticipants.exists { case (participant, _) =>
            participant.adminParty.toLf == adminParty
          },
          (),
          s"Admin party $adminParty not hosted on its transfer-out participant with confirmation permission.",
        )
      }

}

object TransferOutValidationNonTransferringParticipant {

  /* Checks that can be done by a non-transferring participant
   * - every stakeholder is hosted on a participant with an admin party
   * - the admin parties are hosted only on their participants
   */
  def apply(
      request: FullTransferOutTree,
      sourceTopology: TopologySnapshot,
      logger: TracedLogger,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] = {
    val validation = new TransferOutValidationNonTransferringParticipant(
      request,
      sourceTopology,
    ) {}
    for {
      _ <- validation.adminPartiesCanConfirm(logger)
      _ <- validation.stakeholdersHaveTransferringParticipant
    } yield ()
  }

}
