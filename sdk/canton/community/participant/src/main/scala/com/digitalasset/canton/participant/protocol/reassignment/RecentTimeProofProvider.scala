// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  NoTimeProofFromDomain,
  ReassignmentProcessorError,
}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

/** Returns a recent time proof received from the given domain. */
private[reassignment] class RecentTimeProofProvider(
    submissionHandles: DomainId => Option[ReassignmentSubmissionHandle],
    syncCryptoApi: SyncCryptoApiProvider,
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

  def get(targetDomainId: Target[DomainId], staticDomainParameters: Target[StaticDomainParameters])(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, TimeProof] = {
    val domain = targetDomainId.unwrap

    for {
      handle <- EitherT.fromEither[FutureUnlessShutdown](
        submissionHandles(domain).toRight(NoTimeProofFromDomain(domain, "unknown domain"))
      )

      crypto <- EitherT.fromEither[FutureUnlessShutdown](
        syncCryptoApi
          .forDomain(domain, staticDomainParameters.value)
          .toRight(NoTimeProofFromDomain(domain, "getting the crypto client"))
      )

      parameters <- EitherT(
        FutureUnlessShutdown
          .outcomeF(
            crypto.ips.currentSnapshotApproximation.findDynamicDomainParameters()
          )
          .map(
            _.leftMap(err =>
              NoTimeProofFromDomain(domain, s"unable to find domain parameters: $err")
            )
          )
      )

      exclusivityTimeout = parameters.assignmentExclusivityTimeout
      desiredTimeProofFreshness = calculateFreshness(exclusivityTimeout)
      timeProof <- EitherT.right(handle.timeTracker.fetchTimeProof(desiredTimeProofFreshness))
    } yield timeProof
  }
}
