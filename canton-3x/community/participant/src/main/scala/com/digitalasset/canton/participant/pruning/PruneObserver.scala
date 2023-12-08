// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.Eval
import cats.syntax.foldable.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.store.SequencerCounterTrackerStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.{ExecutionContext, Future}

private[participant] class PruneObserver(
    requestJournalStore: RequestJournalStore,
    sequencerCounterTrackerStore: SequencerCounterTrackerStore,
    sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
    acsCommitmentStore: AcsCommitmentStore,
    acs: ActiveContractStore,
    keyJournal: ContractKeyJournal,
    submissionTrackerStore: SubmissionTrackerStore,
    inFlightSubmissionStore: Eval[InFlightSubmissionStore],
    domainId: DomainId,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  /** Stores the participant's local time when we last requested a pruning call
    * (or [[com.digitalasset.canton.data.CantonTimestamp.MinValue]] if unknown)
    */
  private val lastPruneRequest: AtomicReference[CantonTimestamp] = new AtomicReference(
    CantonTimestamp.MinValue
  )
  private val running: AtomicBoolean = new AtomicBoolean(false)
  def observer(
      traceContext: TraceContext
  ): Unit = {
    val localTs = clock.now
    if (lastPruneRequest.updateAndGet(_.max(localTs)) == localTs)
      doFlush(localTs)(traceContext)
  }

  private def doFlush(localTs: CantonTimestamp)(implicit traceContext: TraceContext): Unit = {
    // if we are not closing and not running, then we can start a new prune
    if (!isClosing && running.compareAndSet(false, true)) {
      val runningF = performUnlessClosingF(functionFullName) {
        for {
          safeToPruneTsO <-
            AcsCommitmentProcessor.safeToPrune(
              requestJournalStore,
              sequencerCounterTrackerStore,
              sortedReconciliationIntervalsProvider,
              acsCommitmentStore,
              inFlightSubmissionStore.value,
              domainId,
              checkForOutstandingCommitments = false,
            )
          _ <- safeToPruneTsO.fold(Future.unit)(prune(_))
        } yield ()
      }.onShutdown(()).thereafter { _ =>
        // once we've completed, see if we need to restart the next iteration immediately
        running.set(false)
        val current = lastPruneRequest.get()
        if (current > localTs) {
          doFlush(current)(traceContext)
        }
      }
      FutureUtil.doNotAwait(runningF, "Periodic background journal pruning failed")
    }
  }

  private def prune(pruneTs: CantonTimestampSecond)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    logger.debug(s"Starting periodic background pruning of journals up to ${pruneTs}")
    val acsDescription = s"Periodic ACS prune at $pruneTs:"
    // Clean unused entries from the ACS
    val acsF = performUnlessClosingF(acsDescription)(
      FutureUtil.logOnFailure(
        acs.prune(pruneTs.forgetRefinement),
        acsDescription,
      )
    )
    val journalFDescription = s"Periodic contract key journal prune at $pruneTs: "
    // clean unused contract key journal entries
    val journalF = performUnlessClosingF(journalFDescription)(
      FutureUtil.logOnFailure(
        keyJournal.prune(pruneTs.forgetRefinement),
        journalFDescription,
      )
    )
    val submissionTrackerStoreDescription =
      s"Periodic submission tracker store prune at $pruneTs: "
    // Clean unused entries from the submission tracker store
    val submissionTrackerStoreF = performUnlessClosingF(submissionTrackerStoreDescription)(
      FutureUtil.logOnFailure(
        submissionTrackerStore.prune(pruneTs.forgetRefinement),
        submissionTrackerStoreDescription,
      )
    )
    Seq(acsF, journalF, submissionTrackerStoreF).sequence_.onShutdown(())
  }
}
