// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v2.state_service.GetConnectedSynchronizersRequest
import com.daml.timer.RetryStrategy

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object RetryingGetConnectedSynchronizersForParty {
  // There is a 250ms grace period for all topology changes
  // Remove this wait once proper synchronization is ensured server-side
  def apply(services: LedgerServices, party: String, minSynchronizers: Int)(implicit
      ec: ExecutionContext
  ): Future[Seq[String]] =
    RetryStrategy
      .exponentialBackoff(attempts = 20, 125.millis) { (_, _) =>
        services.state
          .getConnectedSynchronizers(new GetConnectedSynchronizersRequest(party, "", ""))
          .map(_.connectedSynchronizers.map(_.synchronizerId))
          .transform {
            case Success(synchronizers) if synchronizers.sizeIs < minSynchronizers =>
              Failure(
                new RuntimeException(
                  s"Not enough connected synchronizers when allocating party $party. Want $minSynchronizers, got ${synchronizers.size}"
                )
              )
            case other => other
          }
      }
      .recoverWith { case NonFatal(_) =>
        Future.successful(List(""))
      }
}
