// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.{EitherT, Validated, ValidatedNec}
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import cats.{Applicative, MonoidK}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessorError.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.ExecutionContext

/** Holds information about what (admin) parties and participants need to be involved in
  * performing a transfer-out of a certain contract.
  *
  * @param adminParties The admin parties for each transfer-out participant i.e. hosting a signatory
  *                     with confirmation rights on both the source and target domains.
  * @param participants All participants hosting at least one stakeholder (i.e., including observers
  *                     not only signatories), regardless of their permission.
  */
private[protocol] sealed abstract case class AdminPartiesAndParticipants(
    adminParties: Set[LfPartyId],
    participants: Set[ParticipantId],
)

private[protocol] object AdminPartiesAndParticipants {

  def apply(
      contractId: LfContractId,
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      sourceTopology: TopologySnapshot,
      targetTopology: TopologySnapshot,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, AdminPartiesAndParticipants] = {

    for {
      _ <- submitterIsStakeholder(contractId, submitter, stakeholders)
      participantsByParty <- PartyParticipantPermissions(
        stakeholders,
        sourceTopology,
        targetTopology,
      )
      transferOutParticipants <- transferOutParticipants(participantsByParty)
      transferOutAdminParties <- transferOutAdminParties(
        sourceTopology,
        logger,
        transferOutParticipants,
      )
    } yield {

      val participants =
        participantsByParty.perParty.view
          .map(_.source.all)
          .foldLeft(Set.empty[ParticipantId])(_ ++ _)

      new AdminPartiesAndParticipants(transferOutAdminParties, participants) {}
    }
  }

  private def submitterIsStakeholder(
      contractId: LfContractId,
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] =
    condUnitET(
      stakeholders.contains(submitter),
      SubmittingPartyMustBeStakeholderOut(contractId, submitter, stakeholders),
    )

  private def adminParty(
      sourceTopology: TopologySnapshot,
      logger: TracedLogger,
  )(participant: ParticipantId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[ValidatedNec[String, LfPartyId]] = {
    val adminParty = participant.adminParty.toLf
    TransferOutValidationUtil
      .confirmingAdminParticipants(sourceTopology, adminParty, logger)
      .map { adminParticipants =>
        Validated.condNec(
          adminParticipants.get(participant).exists(_.permission.canConfirm),
          adminParty,
          s"Transfer-out participant $participant cannot confirm on behalf of its admin party.",
        )
      }
  }

  private def transferOutAdminParties(
      sourceTopology: TopologySnapshot,
      logger: TracedLogger,
      transferOutParticipants: List[ParticipantId],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Set[LfPartyId]] =
    EitherT(
      transferOutParticipants
        .parTraverse(adminParty(sourceTopology, logger))
        .map(
          _.sequence.toEither
            .bimap(TransferOutProcessorError.fromChain(AdminPartyPermissionErrors), _.toSet)
        )
    )

  /* Computes the transfer-out participants for the transfers and checks the following hosting requirements for each party:
   * - If the party is hosted on a participant with submission permission,
   * then at least one such participant must also have submission permission
   * for that party on the target domain.
   * This ensures that the party can initiate the transfer-in if needed and continue to use the contract on the
   * target domain, unless the permissions change in between.
   * - The party must be hosted on a participant that has confirmation permission
   * on both domains for this party.
   */
  private def transferOutParticipants(
      permissions: PartyParticipantPermissions
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, List[ParticipantId]] = {

    def validate(sourceTs: CantonTimestamp, targetTs: CantonTimestamp)(
        permission: PartyParticipantPermissions.PerParty
    ): ValidatedNec[String, List[ParticipantId]] = {

      val PartyParticipantPermissions.PerParty(party, source, target) = permission

      def hasSubmissionPermission =
        source.submitters.isEmpty || source.submitters.exists(target.submitters.contains)

      def missingPermissionMessage =
        show"For party $party, no participant with submission permission on source domain (at $sourceTs) has submission permission on target domain (at $targetTs)."

      def missingConfirmationOverlapMessage =
        show"No participant of the party $party has confirmation permission on both domains at respective timestamps $sourceTs and $targetTs."

      val transferOutParticipants = source.confirmers.intersect(target.confirmers)

      val submissionPermissionCheck =
        Validated.condNec(hasSubmissionPermission, (), missingPermissionMessage)

      val confirmersOverlap =
        Validated.condNec(
          transferOutParticipants.nonEmpty,
          transferOutParticipants.toList,
          missingConfirmationOverlapMessage,
        )

      Applicative[ValidatedNec[String, *]].productR(submissionPermissionCheck)(confirmersOverlap)
    }

    EitherT.fromEither[FutureUnlessShutdown](
      permissions.perParty
        .traverse(validate(permissions.validityAtSource, permissions.validityAtTarget))
        .bimap(
          TransferOutProcessorError.fromChain(PermissionErrors),
          MonoidK[List].algebra.combineAll,
        )
        .toEither
    )
  }

}
