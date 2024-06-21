// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.configuration

import com.digitalasset.daml.lf.data.Ref
import com.daml.timer.RetryStrategy
import com.digitalasset.canton.ledger.api.domain.ParticipantOffset.Absolute
import com.digitalasset.canton.ledger.participant.state.index.LedgerEndService
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.configuration.LedgerEndObserverFromIndex.*
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordered.orderingToOrdered
import scala.util.control.NoStackTrace

/** Wait for ledger to have an offset that is not before the ledger begin.
  *
  * The [[waitForNonEmptyLedger]] method returns after the first event has been read (and the offset is no longer before
  * the beginning of the ledger), or the timeout expires.
  */
private[apiserver] final class LedgerEndObserverFromIndex(
    indexService: LedgerEndService,
    servicesExecutionContext: ExecutionContext,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def waitForNonEmptyLedger(
      initSyncTimeout: Duration
  ): Future[Unit] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory)(TraceContext.empty)

    val retryAttempts = 20
    val initializationRetryDelay = initSyncTimeout / retryAttempts.toDouble

    RetryStrategy
      .constant(
        attempts = Some(retryAttempts),
        waitTime = initializationRetryDelay,
      )(_ => true) { (_, _) =>
        indexService
          .currentLedgerEnd()
          .flatMap {
            case offset if offset > Absolute(Ref.LedgerString.assertFromString("00")) =>
              logger.info(s"New offset ($offset) greater than ledger begin found.")
              Future.successful(())
            case offset =>
              logger.debug(
                s"The offset ($offset) was not greater than ledger begin, retrying again in $initializationRetryDelay"
              )
              Future.failed(LedgerNotInitialized)
          }(servicesExecutionContext)
      }(servicesExecutionContext)

  }
}

private[apiserver] object LedgerEndObserverFromIndex {

  private object LedgerNotInitialized extends NoStackTrace
}
