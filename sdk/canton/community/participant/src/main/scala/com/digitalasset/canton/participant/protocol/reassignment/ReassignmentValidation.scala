// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.logging.LoggingContext
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ContractValidator, EitherTUtil, MonadUtil, ReassignmentTag}

import scala.concurrent.ExecutionContext

object ReassignmentValidation {

  /**   - check if the submitter is a stakeholder
    *   - check if the submitter is hosted on the participant
    */
  def checkSubmitter(
      reference: ReassignmentRef,
      topologySnapshot: ReassignmentTag[TopologySnapshot],
      submitter: LfPartyId,
      participantId: ParticipantId,
      stakeholders: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    for {
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        stakeholders.contains(submitter),
        ReassignmentValidationError.SubmitterMustBeStakeholder(
          reference,
          submitter,
          stakeholders,
        ),
      )

      _ <- EitherT(
        topologySnapshot.unwrap
          .hostedOn(Set(submitter), participantId)
          .map(_.get(submitter))
          .flatMap {
            case Some(_) =>
              FutureUnlessShutdown.pure(Either.unit)
            case None =>
              FutureUnlessShutdown.pure(
                Left(
                  ReassignmentValidationError.NotHostedOnParticipant(
                    reference,
                    submitter,
                    participantId,
                  ): ReassignmentValidationError
                )
              )
          }
      )
    } yield ()

  def authenticateContractAndStakeholders(
      contractValidator: ContractValidator,
      reassignmentRequest: FullReassignmentViewTree,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] = {
    val declaredViewStakeholders = reassignmentRequest.stakeholders
    val declaredContractStakeholders = reassignmentRequest.contracts.stakeholders

    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Either.cond(
          declaredViewStakeholders == declaredContractStakeholders,
          (),
          ReassignmentValidationError.StakeholdersMismatch(
            reassignmentRequest.reassignmentRef,
            declaredViewStakeholders = declaredViewStakeholders,
            expectedStakeholders = declaredContractStakeholders,
          ): ReassignmentValidationError,
        )
      )

      _ <- MonadUtil.sequentialTraverse(reassignmentRequest.contracts.contracts.forgetNE) {
        reassign =>
          contractValidator
            .authenticate(reassign.contract.inst, reassign.contract.templateId.packageId)(
              ec,
              traceContext,
              LoggingContext.empty,
            )
            .leftMap { reason =>
              ReassignmentValidationError.ContractAuthenticationFailure(
                reassignmentRequest.reassignmentRef,
                reason,
                reassign.contract.contractId,
              ): ReassignmentValidationError
            }
      }
    } yield ()
  }

  def ensureMediatorActive(
      topologySnapshot: ReassignmentTag[TopologySnapshot],
      mediator: MediatorGroupRecipient,
      reassignmentId: ReassignmentId,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError.MediatorInactive, Unit] =
    EitherT(topologySnapshot.unwrap.isMediatorActive(mediator).map { isActive =>
      Either
        .cond(isActive, (), ReassignmentValidationError.MediatorInactive(reassignmentId, mediator))
    })
}
