// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  NoReassignmentSubmissionPermission,
  ReassignmentProcessorError,
}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

private[protocol] object CanSubmitTransfer {

  def unassignment(
      contractId: LfContractId,
      topologySnapshot: TopologySnapshot,
      submitter: LfPartyId,
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    check(s"Unassignment of $contractId", topologySnapshot, submitter, participantId)
      .mapK(FutureUnlessShutdown.outcomeK)

  def assignment(
      reassignmentId: ReassignmentId,
      topologySnapshot: TopologySnapshot,
      submitter: LfPartyId,
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, ReassignmentProcessorError, Unit] =
    check(s"assignment `$reassignmentId`", topologySnapshot, submitter, participantId)

  private def check(
      kind: => String,
      topologySnapshot: TopologySnapshot,
      submitter: LfPartyId,
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, ReassignmentProcessorError, Unit] = {
    lazy val noPermission: ReassignmentProcessorError =
      NoReassignmentSubmissionPermission(kind, submitter, participantId)
    EitherT(
      topologySnapshot
        .hostedOn(Set(submitter), participantId)
        .map(_.get(submitter))
        .flatMap {
          case Some(attribute) if attribute.permission == Submission => Future.successful(Right(()))
          case Some(attribute) if attribute.permission.canConfirm =>
            // We allow transfer submissions by each individual active participants of a consortium party
            topologySnapshot.consortiumThresholds(Set(submitter)).map { thresholds =>
              Either.cond(thresholds.get(submitter).exists(_ > PositiveInt.one), (), noPermission)
            }

          case _ =>
            Future.successful(Left(noPermission))
        }
    )
  }
}
