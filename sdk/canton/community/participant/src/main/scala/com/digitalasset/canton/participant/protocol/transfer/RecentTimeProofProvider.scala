// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.{
  NoTimeProofFromDomain,
  TransferProcessorError,
}
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Returns a recent time proof received from the given domain. */
private[transfer] class RecentTimeProofProvider(
    submissionHandles: DomainId => Option[TransferSubmissionHandle],
    syncCryptoApi: SyncCryptoApiProvider,
    override val loggerFactory: NamedLoggerFactory,
    transferTimeProofFreshnessProportion: NonNegativeInt,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private def calculateFreshness(
      exclusivityTimeout: NonNegativeFiniteDuration
  ): NonNegativeFiniteDuration =
    if (transferTimeProofFreshnessProportion.unwrap == 0)
      NonNegativeFiniteDuration.Zero // always fetch time proof
    else
      exclusivityTimeout / transferTimeProofFreshnessProportion

  def get(targetDomainId: TargetDomainId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, TimeProof] = {
    val domain = targetDomainId.unwrap

    for {
      handle <- EitherT.fromEither[FutureUnlessShutdown](
        submissionHandles(domain).toRight(NoTimeProofFromDomain(domain, "unknown domain"))
      )

      crypto <- EitherT.fromEither[FutureUnlessShutdown](
        syncCryptoApi
          .forDomain(domain)
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

      exclusivityTimeout = parameters.transferExclusivityTimeout
      desiredTimeProofFreshness = calculateFreshness(exclusivityTimeout)
      timeProof <- EitherT.right(handle.timeTracker.fetchTimeProof(desiredTimeProofFreshness))
    } yield timeProof
  }
}
