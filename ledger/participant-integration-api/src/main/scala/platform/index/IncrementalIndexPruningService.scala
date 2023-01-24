// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import cats.data.OptionT
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
    def go(pruningWindowStartExclusive: Offset): OptionT[Future, Unit] =
      for {
        // If start exclusive is not smaller than the requested pruneUpToInclusive, we're done
        _ <- OptionT.when[Future, Unit](pruningWindowStartExclusive < pruneUpToInclusive)(())
        maxWindowOffset <- OptionT(
          // If no offset after `pruningWindowStartExclusive`, nothing else to prune
          ledgerReadDao.getOffsetAfter(
            startExclusive = pruningWindowStartExclusive,
            count = maxPruningWindowSize,
          )
        )
        // Ensure we don't prune beyond the requested pruneUpToInclusive
        pruneTo = Ordering[Offset].min(maxWindowOffset, pruneUpToInclusive)
        _ <- OptionT.liftF(ledgerReadDao.prune(pruneTo, pruneAllDivulgedContracts))
        _ = logger.warn(s"Pruned up to $pruneTo")
        _ <- go(pruningWindowStartExclusive = pruneTo)
      } yield ()

    logger.warn(
      s"Pruning the Index database incrementally (maximum window size of $maxPruningWindowSize) " +
        s"from $latestPrunedUpToInclusive to $pruneUpToInclusive " +
        s"with divulged contracts pruning ${if (pruneAllDivulgedContracts) "enabled" else "disabled"}."
    )
    go(pruningWindowStartExclusive = latestPrunedUpToInclusive).getOrElse(())
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
        case (None, _) => Offset.beforeBegin
        case (Some(prunedAllDivulgence), Some(_)) => prunedAllDivulgence
        case (Some(_), None) =>
          throw new IllegalStateException(
            "Pruning all divulgence present and other is None (TODO pruning rephrase)"
          )
      }
    else pruningOffsets._2.getOrElse(Offset.beforeBegin)
}
