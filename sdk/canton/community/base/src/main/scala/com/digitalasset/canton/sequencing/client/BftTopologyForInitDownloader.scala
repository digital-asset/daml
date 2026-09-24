// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.crypto.topology.TopologyStateHash
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.protocol.{
  TopologyStateForInitHashResponse,
  TopologyStateForInitRequest,
}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** This class encapsulates the logic to download topology txs and verify them using a hash obtained
  * via a separate procedure (e.g. a threshold read from multiple sequencers).
  */
object BftTopologyForInitDownloader {
  def downloadAndVerifyTopologyTxs(
      maxRetries: Int,
      retryLogLevel: Option[Level],
      retryDelay: FiniteDuration,
      request: TopologyStateForInitRequest,
      loggerFactory: NamedLoggerFactory,
      getBftInitTopologyStateHash: TopologyStateForInitRequest => EitherT[
        FutureUnlessShutdown,
        String,
        TopologyStateForInitHashResponse,
      ],
      downloadSnapshot: TopologyStateForInitRequest => EitherT[
        FutureUnlessShutdown,
        String,
        GenericStoredTopologyTransactions,
      ],
  )(implicit
      closeContext: CloseContext,
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, GenericStoredTopologyTransactions] = {
    val logger = loggerFactory.getTracedLogger(this.getClass)
    for {
      expectedBftHash <- {
        EitherT(
          retry
            .Pause(
              logger = logger,
              hasSynchronizeWithClosing = closeContext.context,
              maxRetries = maxRetries,
              delay = retryDelay,
              operationName = "Get hash for init topology state",
              retryLogLevel = retryLogLevel,
            )
            .unlessShutdown(getBftInitTopologyStateHash(request).value, NoExceptionRetryPolicy)
        )
      }
      _ = logger.info(s"Expecting topology state for init with hash $expectedBftHash")
      topologyTransactions <- {
        EitherT(
          retry
            .Pause(
              logger = logger,
              hasSynchronizeWithClosing = closeContext.context,
              maxRetries = maxRetries,
              delay = retryDelay,
              operationName = "Download topology state for init",
              retryLogLevel = retryLogLevel,
            )
            .unlessShutdown(
              {
                val hashBuilder = TopologyStateHash.build()
                downloadSnapshot(request).value.map {
                  case Right(topologyTransactions) =>
                    topologyTransactions.result.foreach { tx =>
                      hashBuilder.add(tx).discard
                    }
                    val computedHash = hashBuilder.finish().hash
                    if (computedHash == expectedBftHash.topologyStateHash) {
                      logger.info(
                        s"Successfully downloaded topology state for init with hash matching expected $computedHash"
                      )
                      Right(topologyTransactions)
                    } else {
                      Left(
                        s"Bft hash mismatch for downloaded topology state for init (hash = $computedHash, expected hash = ${expectedBftHash.topologyStateHash})"
                      )
                    }
                  case err @ Left(_) => err
                }
              },
              NoExceptionRetryPolicy,
            )
        )
      }
    } yield topologyTransactions
  }
}
