// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, SessionEncryptionKeyCacheConfig}
import com.digitalasset.canton.health.{
  AtomicHealthComponent,
  CloseableHealthComponent,
  ComponentHealthState,
}
import com.digitalasset.canton.lifecycle.{LifeCycle, PromiseUnlessShutdownFactory}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ConnectedSynchronizerMetrics
import com.digitalasset.canton.participant.protocol.*
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ConflictDetector,
  NaiveRequestTracker,
  RequestTracker,
  RequestTrackerLookup,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.PendingReassignmentSubmission
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionSynchronizerTracker
import com.digitalasset.canton.participant.store.memory.ReassignmentCache
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** The participant-relevant state and components for a synchronizer that is kept only in memory and
  * must be reconstructed after crashes and fatal errors from the [[SyncPersistentState]]. The
  * ephemeral state can be kept across network disconnects provided that the local processing
  * continues as far as possible.
  */
class SyncEphemeralState(
    participantId: ParticipantId,
    val recordOrderPublisher: RecordOrderPublisher,
    val timeTracker: SynchronizerTimeTracker,
    val inFlightSubmissionSynchronizerTracker: InFlightSubmissionSynchronizerTracker,
    persistentState: SyncPersistentState,
    val ledgerApiIndexer: LedgerApiIndexer,
    val contractStore: ContractStore,
    promiseUSFactory: PromiseUnlessShutdownFactory,
    val startingPoints: ProcessingStartingPoints,
    metrics: ConnectedSynchronizerMetrics,
    exitOnFatalFailures: Boolean,
    sessionKeyCacheConfig: SessionEncryptionKeyCacheConfig,
    override val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    clock: Clock,
)(implicit executionContext: ExecutionContext)
    extends SyncEphemeralStateLookup
    with NamedLogging
    with CloseableHealthComponent
    with AtomicHealthComponent {

  override val name: String = SyncEphemeralState.healthName
  override def initialHealthState: ComponentHealthState = ComponentHealthState.NotInitializedState
  override def closingState: ComponentHealthState =
    ComponentHealthState.failed("Disconnected from synchronizer")

  // Key is the root hash of the reassignment tree
  val pendingUnassignmentSubmissions: TrieMap[RootHash, PendingReassignmentSubmission] =
    TrieMap.empty[RootHash, PendingReassignmentSubmission]
  val pendingAssignmentSubmissions: TrieMap[RootHash, PendingReassignmentSubmission] =
    TrieMap.empty[RootHash, PendingReassignmentSubmission]

  val sessionKeyStore: SessionKeyStore = SessionKeyStore(sessionKeyCacheConfig)

  val requestJournal =
    new RequestJournal(
      persistentState.requestJournalStore,
      metrics,
      loggerFactory,
      startingPoints.processing.nextRequestCounter,
    )
  val requestCounterAllocator = new RequestCounterAllocatorImpl(
    startingPoints.cleanReplay.nextRequestCounter,
    startingPoints.cleanReplay.nextSequencerCounter,
    loggerFactory,
  )

  val reassignmentCache = new ReassignmentCache(
    persistentState.reassignmentStore,
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  val requestTracker: RequestTracker = {
    val conflictDetector = new ConflictDetector(
      persistentState.activeContractStore,
      reassignmentCache,
      loggerFactory,
      persistentState.enableAdditionalConsistencyChecks,
      executionContext,
      exitOnFatalFailures = exitOnFatalFailures,
      timeouts,
      futureSupervisor,
    )

    new NaiveRequestTracker(
      startingPoints.cleanReplay.nextSequencerCounter,
      startingPoints.cleanReplay.prenextTimestamp,
      conflictDetector,
      promiseUSFactory,
      metrics.conflictDetection,
      exitOnFatalFailures = exitOnFatalFailures,
      timeouts,
      loggerFactory,
      futureSupervisor,
      clock,
    )
  }

  val phase37Synchronizer =
    new Phase37Synchronizer(
      loggerFactory,
      futureSupervisor,
      timeouts,
    )

  val submissionTracker: SubmissionTracker =
    SubmissionTracker(
      participantId,
      persistentState.submissionTrackerStore,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )

  def markAsRecovered()(implicit tc: TraceContext): Unit =
    resolveUnhealthy()

  override def onClosed(): Unit =
    LifeCycle.close(
      requestTracker,
      recordOrderPublisher,
      submissionTracker,
      phase37Synchronizer,
      reassignmentCache,
    )(logger)

}

object SyncEphemeralState {
  val healthName: String = "sync-ephemeral-state"
}

trait SyncEphemeralStateLookup {
  this: SyncEphemeralState =>

  def sessionKeyStoreLookup: SessionKeyStore = sessionKeyStore

  def contractLookup: ContractLookup = contractStore

  def reassignmentLookup: ReassignmentLookup = reassignmentCache

  def tracker: RequestTrackerLookup = requestTracker
}
