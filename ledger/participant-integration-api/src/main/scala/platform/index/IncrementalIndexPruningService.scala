// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.dao.LedgerReadDao

import scala.concurrent.{ExecutionContext, Future}

class IncrementalIndexPruningService(ledgerReadDao: LedgerReadDao, maximumPruningWindowSize: Int) {
  private val logger = ContextualizedLogger.get(getClass)

  def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    for {
      pruningOffsets <- ledgerReadDao.pruningOffsets
      startPruningOffset = getStartPruningOffset(pruneAllDivulgedContracts, pruningOffsets)

      _ <- pruneIncrementally(
        latestPrunedUpToInclusive = startPruningOffset,
        pruneUpToInclusive = pruneUpToInclusive,
        pruneAllDivulgedContracts = pruneAllDivulgedContracts,
        maxPruningWindowSize = maximumPruningWindowSize,
      )
    } yield ()
  }

  private def pruneIncrementally(
      latestPrunedUpToInclusive: Offset,
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      maxPruningWindowSize: Int,
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Future[Unit] = {
    def go(pruningWindowStartExclusive: Offset): Future[Unit] =
      // If start exclusive is not smaller than the requested `pruneUpToInclusive`, we're done
      if (pruningWindowStartExclusive < pruneUpToInclusive)
        for {
          maxWindowOffset <- ledgerReadDao
            .getOffsetAfter(
              startExclusive = pruningWindowStartExclusive,
              count = maxPruningWindowSize,
            )
            // If no offset after `pruningWindowStartExclusive`,
            // use `pruneUpToInclusive` to cover the entire requested range and ensure termination
            .map(_.getOrElse(pruneUpToInclusive))
          // Ensure we don't prune beyond the requested pruneUpToInclusive
          pruneNextWindowUpTo = Ordering[Offset].min(maxWindowOffset, pruneUpToInclusive)
          _ = logger.info(s"Pruned up to $pruneNextWindowUpTo")
          _ <- go(pruningWindowStartExclusive = pruneNextWindowUpTo)
        } yield ()
      else Future.unit

    logger.info(
      s"Pruning the Index database incrementally (maximum window size of $maxPruningWindowSize) " +
        s"from $latestPrunedUpToInclusive to $pruneUpToInclusive " +
        s"with divulged contracts pruning ${if (pruneAllDivulgedContracts) "enabled" else "disabled"}."
    )

    go(pruningWindowStartExclusive = latestPrunedUpToInclusive)
  }

  // Computes the starting offset for the incremental pruning.
  // If divulgence pruning is enabled,
  // the latest divulged contracts pruned up to inclusive is chosen
  // (as it's guaranteed to be smaller than pruned_up_to_inclusive)
  private def getStartPruningOffset(
      pruneAllDivulgedContracts: Boolean,
      pruningOffsets: (Option[Offset], Option[Offset]),
  ): Offset =
    if (pruneAllDivulgedContracts)
      pruningOffsets match {
        case (_, None) => Offset.beforeBegin
        case (Some(_), Some(prunedAllDivulgence)) => prunedAllDivulgence
        case (None, Some(_)) =>
          throw new IllegalStateException(
            "Pruning all divulgence present and other is None (TODO pruning rephrase)"
          )
      }
    else pruningOffsets._2.getOrElse(Offset.beforeBegin)
}
