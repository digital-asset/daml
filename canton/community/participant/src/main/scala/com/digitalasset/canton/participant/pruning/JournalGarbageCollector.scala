// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.Eval
import cats.syntax.foldable.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestampSecond
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.store.SequencerCounterTrackerStore
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Canton synchronisation journals garbage collectors
  *
  * The difference between the normal ledger pruning feature and the journal garbage collector is that
  * the ledger pruning is configured and invoked by the user, whereas the journal garbage collector runs
  * periodically in the background, where the retention period is generally not configurable.
  */
private[participant] class JournalGarbageCollector(
    requestJournalStore: RequestJournalStore,
    sequencerCounterTrackerStore: SequencerCounterTrackerStore,
    sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
    acsCommitmentStore: AcsCommitmentStore,
    acs: ActiveContractStore,
    keyJournal: ContractKeyJournal,
    submissionTrackerStore: SubmissionTrackerStore,
    inFlightSubmissionStore: Eval[InFlightSubmissionStore],
    domainId: DomainId,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends JournalGarbageCollector.Scheduler {

  def observer(
      traceContext: TraceContext
  ): Unit = flush(traceContext)

  override protected def run()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    performUnlessClosingF(functionFullName) {
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

private[pruning] object JournalGarbageCollector {
  private[pruning] abstract class Scheduler
      extends NamedLogging
      with FlagCloseable
      with HasCloseContext {

    /** Manage internal state of the collector
      *
      * @param request if true, then the acs commitment processor completed a commitment period and suggested to kick off pruning
      * @param locks number of locks that are currently active preventing pruning
      * @param running if set, then a prune is currently running and the promise will be completed once it is done
      */
    private case class State(requested: Boolean, locks: Int, running: Option[Promise[Unit]]) {
      def incrementLock: State = copy(locks = locks + 1)
      def decrementLock: State = copy(locks = Math.max(0, locks - 1))
    }

    private val state: AtomicReference[State] = new AtomicReference(
      State(requested = false, locks = 0, running = None)
    )

    protected def run()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
    protected implicit def executionContext: ExecutionContext

    private[pruning] def flush(
        traceContext: TraceContext
    ): Unit = {
      // set request flag and kick off pruning if flag was not already set
      if (!state.getAndUpdate(_.copy(requested = true)).requested)
        doFlush()(traceContext)
    }

    /** Temporarily turn off journal pruning (in order to download an ACS)
      *
      * This will add one lock. The lock will be removed when [[removeOneLock]] is called.
      * Journal cleaning will resume once all locks are removed
      */
    def addOneLock()(implicit traceContext: TraceContext): Future[Unit] = {
      val old = state.getAndUpdate(_.incrementLock)
      logger.debug(s"Journal garbage collection is now blocked with ${old.locks + 1} locks")
      old.running.map(_.future).getOrElse(Future.unit)
    }

    def removeOneLock()(implicit traceContext: TraceContext): Unit = {
      val old = state.getAndUpdate(_.decrementLock)
      logger.debug(s"Journal garbage collection has now ${old.locks - 1} locks")
      if (old.locks == 1) {
        doFlush()
      }
    }

    private def doFlush()(implicit traceContext: TraceContext): Unit = {
      // if we are not closing and not running, then we can start a new prune
      if (!isClosing) {
        val currentState = state.getAndUpdate {
          // start new process if idle and not blocked
          case State(true, 0, None) => State(requested = false, 0, Some(Promise()))
          // not enabled or already running, do nothing
          case x => x
        }
        if (currentState.locks == 0 && currentState.running.isEmpty) {
          // we are enabled and not running, so start a new prune
          val runningF = run().onShutdown(()).thereafter { _ =>
            // once we've completed, see if we need to restart the next iteration immediately
            val current = state.getAndUpdate(_.copy(running = None))
            current.running.foreach(_.success(()))
            if (current.requested) {
              doFlush()(traceContext)
            }
          }
          FutureUtil.doNotAwait(runningF, "Periodic background journal pruning failed")
        }
      }
    }

  }
}
