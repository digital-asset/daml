// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  NoTimeProofFromSynchronizer,
  ReassignmentProcessorError,
}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

/** Returns a recent time proof received from the given synchronizer. */
private[reassignment] class RecentTimeProofProvider(
    submissionHandles: SynchronizerId => Option[ReassignmentSubmissionHandle],
    syncCryptoApi: SyncCryptoApiParticipantProvider,
    override val loggerFactory: NamedLoggerFactory,
    reassignmentTimeProofFreshnessProportion: NonNegativeInt,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private def calculateFreshness(
      exclusivityTimeout: NonNegativeFiniteDuration
  ): NonNegativeFiniteDuration =
    if (reassignmentTimeProofFreshnessProportion.unwrap == 0)
      NonNegativeFiniteDuration.Zero // always fetch time proof
    else
      exclusivityTimeout / reassignmentTimeProofFreshnessProportion

  def get(
      targetSynchronizerId: Target[SynchronizerId],
      staticSynchronizerParameters: Target[StaticSynchronizerParameters],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, TimeProof] = {
    val synchronizer = targetSynchronizerId.unwrap
    val physicalId =
      PhysicalSynchronizerId(targetSynchronizerId.unwrap, staticSynchronizerParameters.unwrap)

    for {
      handle <- EitherT.fromEither[FutureUnlessShutdown](
        submissionHandles(synchronizer).toRight(
          NoTimeProofFromSynchronizer(physicalId, "unknown synchronizer")
        )
      )

      crypto <- EitherT.fromEither[FutureUnlessShutdown](
        syncCryptoApi
          .forSynchronizer(synchronizer, staticSynchronizerParameters.value)
          .toRight(NoTimeProofFromSynchronizer(physicalId, "getting the crypto client"))
      )

      parameters <- EitherT(
        crypto.ips.currentSnapshotApproximation
          .findDynamicSynchronizerParameters()
          .map(
            _.leftMap(err =>
              NoTimeProofFromSynchronizer(
                physicalId,
                s"unable to find synchronizer parameters: $err",
              )
            )
          )
      )

      exclusivityTimeout = parameters.assignmentExclusivityTimeout
      desiredTimeProofFreshness = calculateFreshness(exclusivityTimeout)
      timeProof <- EitherT.right(handle.timeTracker.fetchTimeProof(desiredTimeProofFreshness))
    } yield timeProof
  }
}
