// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.{EitherT, Validated, ValidatedNec}
import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.FullUnassignmentTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.transfer.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.transfer.UnassignmentProcessorError.{
  AdminPartyPermissionErrors,
  StakeholderHostingErrors,
  fromChain,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

private[transfer] sealed abstract case class UnassignmentValidationNonReassigningParticipant(
    request: FullUnassignmentTree,
    sourceTopology: TopologySnapshot,
) {

  private def stakeholdersHaveReassigningParticipant(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    EitherT(
      FutureUnlessShutdown.outcomeF(
        sourceTopology.activeParticipantsOfPartiesWithInfo(request.stakeholders.toList).map {
          partyWithParticipants =>
            partyWithParticipants.toList
              .traverse_ { case (stakeholder, partyInfo) =>
                val hasReassigningParticipant =
                  partyInfo.participants.exists { case (participant, attributes) =>
                    attributes.permission.canConfirm && request.adminParties.contains(
                      participant.adminParty.toLf
                    )
                  }
                Validated.condNec(
                  hasReassigningParticipant,
                  (),
                  s"Stakeholder $stakeholder has no reassigning participant.",
                )

              }
              .toEither
              .leftMap(fromChain(StakeholderHostingErrors))
        }
      )
    )

  private def adminPartiesCanConfirm(logger: TracedLogger)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, List[Unit]] =
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
    UnassignmentValidationUtil
      .confirmingAdminParticipants(sourceTopology, adminParty, logger)
      .map { adminParticipants =>
        Validated.condNec(
          adminParticipants.exists { case (participant, _) =>
            participant.adminParty.toLf == adminParty
          },
          (),
          s"Admin party $adminParty not hosted on its unassignment participant with confirmation permission.",
        )
      }

}

object UnassignmentValidationNonReassigningParticipant {

  /* Checks that can be done by a non-reassigning participant
   * - every stakeholder is hosted on a participant with an admin party
   * - the admin parties are hosted only on their participants
   */
  def apply(
      request: FullUnassignmentTree,
      sourceTopology: TopologySnapshot,
      logger: TracedLogger,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val validation = new UnassignmentValidationNonReassigningParticipant(
      request,
      sourceTopology,
    ) {}
    for {
      _ <- validation.adminPartiesCanConfirm(logger)
      _ <- validation.stakeholdersHaveReassigningParticipant
    } yield ()
  }

}
