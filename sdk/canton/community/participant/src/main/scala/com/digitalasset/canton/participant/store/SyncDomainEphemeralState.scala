// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, SessionEncryptionKeyCacheConfig}
import com.digitalasset.canton.health.{
  AtomicHealthComponent,
  CloseableHealthComponent,
  ComponentHealthState,
}
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.*
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ConflictDetector,
  NaiveRequestTracker,
  RequestTracker,
  RequestTrackerLookup,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.PendingReassignmentSubmission
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionDomainTracker
import com.digitalasset.canton.participant.store.memory.ReassignmentCache
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** The state of a synchronization domain that is kept only in memory and must be reconstructed after crashes
  * and fatal errors from the [[SyncDomainPersistentState]]. The ephemeral state can be kept across network disconnects
  * provided that the local processing continues as far as possible.
  */
class SyncDomainEphemeralState(
    participantId: ParticipantId,
    val recordOrderPublisher: RecordOrderPublisher,
    val timeTracker: DomainTimeTracker,
    val inFlightSubmissionDomainTracker: InFlightSubmissionDomainTracker,
    persistentState: SyncDomainPersistentState,
    val ledgerApiIndexer: LedgerApiIndexer,
    val startingPoints: ProcessingStartingPoints,
    metrics: SyncDomainMetrics,
    exitOnFatalFailures: Boolean,
    sessionKeyCacheConfig: SessionEncryptionKeyCacheConfig,
    override val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    clock: Clock,
)(implicit executionContext: ExecutionContext)
    extends SyncDomainEphemeralStateLookup
    with NamedLogging
    with CloseableHealthComponent
    with AtomicHealthComponent {

  override val name: String = SyncDomainEphemeralState.healthName
  override def initialHealthState: ComponentHealthState = ComponentHealthState.NotInitializedState
  override def closingState: ComponentHealthState =
    ComponentHealthState.failed("Disconnected from domain")

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
      futureSupervisor,
    )
  val requestCounterAllocator = new RequestCounterAllocatorImpl(
    startingPoints.cleanReplay.nextRequestCounter,
    startingPoints.cleanReplay.nextSequencerCounter,
    loggerFactory,
  )

  val contractStore: ContractStore = persistentState.contractStore

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
    SubmissionTracker(persistentState.staticDomainParameters.protocolVersion)(
      participantId,
      persistentState.submissionTrackerStore,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )

  def markAsRecovered()(implicit tc: TraceContext): Unit =
    resolveUnhealthy()

  override def onClosed(): Unit =
    Lifecycle.close(
      requestTracker,
      recordOrderPublisher,
      submissionTracker,
      phase37Synchronizer,
      reassignmentCache,
    )(logger)

}

object SyncDomainEphemeralState {
  val healthName: String = "sync-domain-ephemeral"
}

trait SyncDomainEphemeralStateLookup {
  this: SyncDomainEphemeralState =>

  def sessionKeyStoreLookup: SessionKeyStore = sessionKeyStore

  def contractLookup: ContractLookup = contractStore

  def reassignmentLookup: ReassignmentLookup = reassignmentCache

  def tracker: RequestTrackerLookup = requestTracker
}
