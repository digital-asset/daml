// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.participant.state.RepairUpdate
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizersLookup,
  SyncEphemeralStateFactory,
  SyncPersistentStateLookup,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.PekkoUtil.FutureQueue
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import scala.Ordered.orderingToOrdered
import scala.concurrent.{ExecutionContext, Future}

/** Common helpers to [[RepairService]] and [[RepairServiceContractsImporter]]
  */
private[repair] final class RepairServiceHelpers(
    participantId: ParticipantId,
    packageMetadataView: PackageMetadataView,
    ledgerApiIndexer: Eval[LedgerApiIndexer],
    parameters: ParticipantNodeParameters,
    syncPersistentStateLookup: SyncPersistentStateLookup,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    @VisibleForTesting
    private[canton] val executionQueue: SimpleExecutionQueue,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  def synchronizerNotConnected(
      psid: PhysicalSynchronizerId
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT.cond(
      !connectedSynchronizersLookup.isConnected(psid),
      (),
      s"Participant is still connected to synchronizer $psid",
    )

  def logOnFailureWithInfoLevel[T](f: FutureUnlessShutdown[T], errorMessage: => String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, T] =
    EitherT.right(
      FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(f, errorMessage, level = Level.INFO)
    )

  /** Retrieve synchronizer information needed to process repair request.
    *
    * The repair request gets inserted at the reprocessing starting point. We use the
    * prenextTimestamp such that a regular request is always the first request for a given
    * timestamp. This is needed for causality tracking, which cannot use a tie-breaker on
    * timestamps. If this repair request succeeds, it will advance the clean RequestIndex to this
    * time of change. That's why it is important that there are no in-flight validation requests
    * before the repair request.
    */
  def readSynchronizerData(
      synchronizerId: SynchronizerId
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, RepairRequest.SynchronizerData] =
    for {
      psid <- EitherT.fromEither[FutureUnlessShutdown](
        syncPersistentStateLookup
          .latestKnownPSId(synchronizerId)
          .toRight(s"Unable to resolve $synchronizerId to a physical synchronizer id")
      )

      _ = logger.debug(s"Using $psid as $synchronizerId")

      persistentState <- EitherT.fromEither[FutureUnlessShutdown](
        lookUpSynchronizerPersistence(psid)
      )
      synchronizerIndex <- EitherT
        .right(
          ledgerApiIndexer.value.ledgerApiStore.value.cleanSynchronizerIndex(synchronizerId)
        )
      topologyFactory <- syncPersistentStateLookup
        .topologyFactoryFor(psid)
        .toRight(s"No topology factory for synchronizer $psid")
        .toEitherT[FutureUnlessShutdown]

      topologySnapshot = topologyFactory.createTopologySnapshot(
        SyncEphemeralStateFactory.currentRecordTime(synchronizerIndex),
        new PackageDependencyResolverImpl(participantId, packageMetadataView, loggerFactory),
        preferCaching = true,
      )
      synchronizerParameters <- OptionT(persistentState.parameterStore.lastParameters)
        .toRight(s"No static synchronizer parameters found for $psid")
    } yield RepairRequest.SynchronizerData(
      psid,
      topologySnapshot,
      persistentState,
      synchronizerParameters,
      SyncEphemeralStateFactory.currentRecordTime(synchronizerIndex),
      SyncEphemeralStateFactory.nextRepairCounter(synchronizerIndex),
      synchronizerIndex,
    )

  def repairCounterSequence(
      fromInclusive: RepairCounter,
      length: PositiveInt,
  ): Either[String, NonEmpty[Seq[RepairCounter]]] =
    for {
      rcs <- Seq
        .iterate(fromInclusive.asRight[String], length.value)(_.flatMap(_.increment))
        .sequence
      ne <- NonEmpty.from(rcs).toRight("generated an empty collection with PositiveInt length")
    } yield ne

  /** Repair commands are inserted where processing starts again upon reconnection.
    *
    * @param synchronizerId
    *   The ID of the synchronizer for which the request is valid
    * @param repairCountersToAllocate
    *   The number of repair counters to allocate in order to fulfill the request
    */
  def initRepairRequestAndVerifyPreconditions(
      synchronizerId: SynchronizerId,
      repairCountersToAllocate: PositiveInt = PositiveInt.one,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, RepairRequest] =
    for {
      synchronizerData <- readSynchronizerData(synchronizerId)
      repairRequest <- initRepairRequestAndVerifyPreconditions(
        synchronizerData,
        repairCountersToAllocate,
      )
    } yield repairRequest

  /** Ensures repair can be run.
    * @return
    *   Next RepairCounter
    */
  def verifyRepairPreconditions(
      synchronizer: RepairRequest.SynchronizerData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, RepairCounter] = {
    val rtRepair = RecordTime.fromTimeOfChange(
      TimeOfChange(
        synchronizer.currentRecordTime,
        Some(synchronizer.nextRepairCounter),
      )
    )
    logger.debug(s"Starting repair request on ${synchronizer.persistentState.psid} at $rtRepair.")

    for {
      _ <- EitherT
        .right(
          SyncEphemeralStateFactory
            .cleanupPersistentState(synchronizer.persistentState, synchronizer.synchronizerIndex)
        )

      incrementalAcsSnapshotWatermark <- EitherT.right(
        synchronizer.persistentState.acsCommitmentStore.runningCommitments.watermark
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        rtRepair > incrementalAcsSnapshotWatermark,
        (),
        s"""Cannot apply a repair command as the incremental acs snapshot is already at $incrementalAcsSnapshotWatermark
           |and the repair command would be assigned a record time of $rtRepair.
           |Reconnect to the synchronizer to reprocess inflight validation requests and retry repair afterwards.""".stripMargin,
      )
    } yield synchronizer.nextRepairCounter
  }

  /** Ensures repair can be run and return [[RepairRequest]]
    */
  def initRepairRequestAndVerifyPreconditions(
      synchronizer: RepairRequest.SynchronizerData,
      repairCountersToAllocate: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, RepairRequest] = {
    val rtRepair = RecordTime.fromTimeOfChange(
      TimeOfChange(
        synchronizer.currentRecordTime,
        Some(synchronizer.nextRepairCounter),
      )
    )
    logger.debug(s"Starting repair request on ${synchronizer.persistentState.psid} at $rtRepair.")

    for {
      nextRepairCounter <- verifyRepairPreconditions(synchronizer)
      repairCounters <- EitherT.fromEither[FutureUnlessShutdown](
        repairCounterSequence(
          nextRepairCounter,
          repairCountersToAllocate,
        )
      )
    } yield RepairRequest(synchronizer, repairCounters)
  }

  /** Read the ACS state for each contract in cids
    * @return
    *   The list of states or an error if one of the states cannot be read. Note that the returned
    *   Seq has same ordering and cardinality of cids
    */
  def readContractAcsStates(
      persistentState: SyncPersistentState,
      cids: Seq[LfContractId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[Option[ActiveContractStore.Status]]] =
    persistentState.activeContractStore
      .fetchStates(cids)
      .map { states =>
        cids.map(cid => states.get(cid).map(_.status))
      }

  // Looks up synchronizer persistence erroring if synchronizer is based on in-memory persistence for which repair is not supported.
  def lookUpSynchronizerPersistence(
      psid: PhysicalSynchronizerId
  ): Either[String, SyncPersistentState] =
    for {
      dp <- syncPersistentStateLookup
        .get(psid)
        .toRight(s"Could not find persistent state for $psid")
      _ <- Either.cond(
        !dp.isMemory,
        (),
        s"$psid is in memory which is not supported by repair. Use db persistence.",
      )
    } yield dp

  def runConsecutiveAndAwaitUS[B](
      description: String,
      code: => EitherT[FutureUnlessShutdown, String, B],
  )(implicit
      traceContext: TraceContext
  ): Either[String, B] = {
    logger.info(s"Queuing $description")

    // repair commands can take an unbounded amount of time
    parameters.processingTimeouts.unbounded.awaitUS(description) {
      logger.info(s"Queuing $description")
      executionQueue.executeEUS(code, description).value
    }
  }.onShutdown(throw GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)

  def runConsecutive[B](
      description: String,
      code: => EitherT[FutureUnlessShutdown, String, B],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, B] = {
    logger.info(s"Queuing $description")
    executionQueue.executeEUS(code, description)
  }

  def withRepairIndexer(code: FutureQueue[RepairUpdate] => EitherT[Future, String, Unit])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    if (connectedSynchronizersLookup.isConnectedToAny) {
      EitherT.leftT[FutureUnlessShutdown, Unit](
        "There are still synchronizers connected. Please disconnect all synchronizers."
      )
    } else {
      ledgerApiIndexer.value.withRepairIndexer(code)
    }
}
