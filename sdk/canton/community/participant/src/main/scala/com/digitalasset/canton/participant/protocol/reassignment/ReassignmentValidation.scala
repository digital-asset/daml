// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.ContractAuthenticator
import com.digitalasset.canton.protocol.Stakeholders
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ReassignmentTag}

import scala.concurrent.ExecutionContext

private[reassignment] class ReassignmentValidation(contractAuthenticator: ContractAuthenticator) {
  def checkMetadata(reassignmentRequest: FullReassignmentViewTree)(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] = {

    val declaredViewStakeholders = reassignmentRequest.stakeholders
    val declaredContractStakeholders = Stakeholders(reassignmentRequest.contract.metadata)

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
      _ <- contractAuthenticator
        .authenticateSerializable(reassignmentRequest.contract)
        .leftMap(error =>
          ReassignmentValidationError.ContractIdAuthenticationFailure(
            reassignmentRequest.reassignmentRef,
            error,
            reassignmentRequest.contractId,
          )
        )
    } yield ())
  }
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
}
