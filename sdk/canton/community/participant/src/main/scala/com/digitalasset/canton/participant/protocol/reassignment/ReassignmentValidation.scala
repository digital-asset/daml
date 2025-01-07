// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.EngineController.GetEngineAbortStatus
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ReassignmentTag}
import com.digitalasset.daml.lf.engine.Error as LfError
import com.digitalasset.daml.lf.interpretation.Error as LfInterpretationError

import scala.concurrent.{ExecutionContext, Future}

private[reassignment] class ReassignmentValidation(engine: DAMLe)(implicit
    val ec: ExecutionContext
) {

  def checkMetadata(
      reassignmentRequest: FullReassignmentViewTree,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit traceContext: TraceContext): EitherT[Future, ReassignmentValidationError, Unit] = {
    val reassignmentRef = reassignmentRequest.reassignmentRef

    val declaredContractMetadata = reassignmentRequest.contract.metadata
    val declaredViewStakeholders = reassignmentRequest.stakeholders

    for {
      recomputedMetadata <- engine
        .contractMetadata(
          reassignmentRequest.contract.contractInstance,
          declaredContractMetadata.stakeholders,
          getEngineAbortStatus,
        )
        .leftMap {
          case DAMLe.EngineError(
                LfError.Interpretation(
                  e @ LfError.Interpretation.DamlException(
                    LfInterpretationError.FailedAuthorization(_, _)
                  ),
                  _,
                )
              ) =>
            ReassignmentValidationError.ReinterpretationError(
              reassignmentRef,
              reason = e.message,
            )
          case DAMLe.EngineError(error) => ReassignmentValidationError.MetadataNotFound(error)
          case DAMLe.EngineAborted(reason) =>
            ReassignmentValidationError.ReinterpretationAborted(reassignmentRef, reason)
        }

      _ <- EitherTUtil.condUnitET[Future](
        recomputedMetadata == declaredContractMetadata,
        ReassignmentValidationError.ContractMetadataMismatch(
          reassignmentRef = reassignmentRef,
          declaredContractMetadata = declaredContractMetadata,
          expectedMetadata = recomputedMetadata,
        ),
      )

      // this one is validated against the recomputed the metadata, so it's the correct one.
      declaredContractStakeholders = Stakeholders(declaredContractMetadata)

      _ <- EitherTUtil
        .condUnitET[Future](
          declaredViewStakeholders == declaredContractStakeholders,
          ReassignmentValidationError.StakeholdersMismatch(
            reassignmentRef,
            declaredViewStakeholders = declaredViewStakeholders,
            expectedStakeholders = declaredContractStakeholders,
          ),
        )
        .leftWiden[ReassignmentValidationError]
    } yield ()
  }

}

object ReassignmentValidation {

  /** - check if the submitter is a stakeholder
    * - check if the submitter is hosted on the participant
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
}
