// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ParsedReassignmentRequest,
  ReassignmentProcessorError,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationResult.CommonValidationResult
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ContractAuthenticator, EitherTUtil, MonadUtil, ReassignmentTag}

import scala.concurrent.ExecutionContext

private[reassignment] trait ReassignmentValidation[
    View <: FullReassignmentViewTree,
    CommonResult <: ReassignmentValidationResult.CommonValidationResult,
    ReassigningParticipantResult
      <: ReassignmentValidationResult.ReassigningParticipantValidationResult,
] {
  type ReassigningParticipantValidationData

  /** The common validations that are performed on all participants (reassigning as well as
    * non-reassigning)
    */
  def performCommonValidations(
      parsedRequest: ParsedReassignmentRequest[View],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CommonValidationResult]

  /** The validations that are performed only for reassigning participants. We need specific
    * parameters depending on the type of reassignment request.
    */
  def performValidationForReassigningParticipants(
      parsedRequest: ParsedReassignmentRequest[View],
      additionalParams: ReassigningParticipantValidationData,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    ReassigningParticipantResult,
  ]
}

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

  def checkMetadata(
      contractAuthenticator: ContractAuthenticator,
      reassignmentRequest: FullReassignmentViewTree,
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] = {
    val declaredViewStakeholders = reassignmentRequest.stakeholders
    val declaredContractStakeholders = reassignmentRequest.contracts.stakeholders

    EitherT.fromEither(for {
      _ <- Either.cond(
        declaredViewStakeholders == declaredContractStakeholders,
        (),
        ReassignmentValidationError.StakeholdersMismatch(
          reassignmentRequest.reassignmentRef,
          declaredViewStakeholders = declaredViewStakeholders,
          expectedStakeholders = declaredContractStakeholders,
        ),
      )
      _ <- MonadUtil.sequentialTraverse(reassignmentRequest.contracts.contracts) { reassign =>
        contractAuthenticator
          .legacyAuthenticate(reassign.contract.inst)
          .leftMap(error =>
            ReassignmentValidationError.ContractIdAuthenticationFailure(
              reassignmentRequest.reassignmentRef,
              error,
              reassign.contract.contractId,
            )
          )
      }
    } yield ())
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
