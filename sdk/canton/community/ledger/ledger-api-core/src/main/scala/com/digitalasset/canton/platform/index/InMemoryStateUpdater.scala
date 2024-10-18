// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import cats.data.NonEmptyVector
import cats.implicits.toBifunctorOps
import com.daml.executors.InstrumentedExecutors
import com.daml.ledger.resources.ResourceOwner
import com.daml.timer.FutureCheck.*
import com.digitalasset.canton.data.DeduplicationPeriod.{DeduplicationDuration, DeduplicationOffset}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, Reassignment, Update}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.index.InMemoryStateUpdater.{PrepareResult, UpdaterFlow}
import com.digitalasset.canton.platform.indexer.TransactionTraversalUtils
import com.digitalasset.canton.platform.store.CompletionFromTransaction
import com.digitalasset.canton.platform.store.cache.OffsetCheckpoint
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.{Contract, InMemoryState, Key, Party}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.daml.lf.ledger.EventId
import com.digitalasset.daml.lf.transaction.Node.{Create, Exercise}
import com.digitalasset.daml.lf.transaction.NodeId
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Sink, Source}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/** Builder of the in-memory state updater Pekko flow.
  *
  * This flow is attached at the end of the Indexer pipeline,
  * consumes the [[com.digitalasset.canton.ledger.participant.state.Update]]s (that have been ingested by the Indexer
  * into the Index database) for populating the Ledger API server in-memory state (see [[InMemoryState]]).
  */
private[platform] object InMemoryStateUpdaterFlow {

  private[index] def apply(
      prepareUpdatesParallelism: Int,
      prepareUpdatesExecutionContext: ExecutionContext,
      updateCachesExecutionContext: ExecutionContext,
      preparePackageMetadataTimeOutWarning: FiniteDuration,
      offsetCheckpointCacheUpdateInterval: FiniteDuration,
      metrics: LedgerApiServerMetrics,
      logger: TracedLogger,
  )(
      inMemoryState: InMemoryState,
      prepare: (Vector[(Offset, Traced[Update])], Long, CantonTimestamp) => PrepareResult,
      update: (PrepareResult, Boolean) => Unit,
  )(implicit traceContext: TraceContext): UpdaterFlow = { repairMode =>
    Flow[(Vector[(Offset, Traced[Update])], Long, CantonTimestamp)]
      .filter(_._1.nonEmpty)
      .via(updateOffsetCheckpointCacheFlow(inMemoryState, offsetCheckpointCacheUpdateInterval))
      .mapAsync(prepareUpdatesParallelism) {
        case (batch, lastEventSequentialId, lastPublicationTime) =>
          Future {
            batch -> prepare(batch, lastEventSequentialId, lastPublicationTime)
          }(prepareUpdatesExecutionContext)
            .checkIfComplete(preparePackageMetadataTimeOutWarning)(
              logger.warn(
                s"Package Metadata View live update did not finish in ${preparePackageMetadataTimeOutWarning.toMillis}ms"
              )
            )
      }
      .async
      .mapAsync(1) { case (batch, result) =>
        Future {
          update(result, repairMode)
          metrics.index.ledgerEndSequentialId.updateValue(result.lastEventSequentialId)
          batch
        }(updateCachesExecutionContext)
      }
  }

  private def updateOffsetCheckpointCacheFlow(
      inMemoryState: InMemoryState,
      interval: FiniteDuration,
  ): Flow[
    (Vector[(Offset, Traced[Update])], Long, CantonTimestamp),
    (Vector[(Offset, Traced[Update])], Long, CantonTimestamp),
    NotUsed,
  ] = {
    // tick source so that we update offset checkpoint caches
    // tick is denoted by None while the rest elements are encapsulated into a Some
    val tick = Source
      .tick(interval, interval, None: Option[Nothing])
      .mapMaterializedValue(_ => NotUsed)

    updateOffsetCheckpointCacheFlowWithTickingSource(inMemoryState.offsetCheckpointCache.push, tick)
  }

  private[index] def updateOffsetCheckpointCacheFlowWithTickingSource(
      updateOffsetCheckpointCache: OffsetCheckpoint => Unit,
      tick: Source[Option[Nothing], NotUsed],
  ): Flow[
    (Vector[(Offset, Traced[Update])], Long, CantonTimestamp),
    (Vector[(Offset, Traced[Update])], Long, CantonTimestamp),
    NotUsed,
  ] =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits.*
      // this flow emits the original stream as is while at the same time broadcasts its elements
      // through a secondary flow that updates the offset checkpoint cache
      // the secondary flow keeps only the Offsets and the Updates of the original stream and merges
      // them with a tick source that ticks every interval seconds to signify the update of the cache

      val broadcast =
        builder.add(Broadcast[(Vector[(Offset, Traced[Update])], Long, CantonTimestamp)](2))

      val merge = builder.add(Merge[Option[(Offset, Update)]](inputPorts = 2, eagerComplete = true))

      val preprocess: Flow[(Vector[(Offset, Traced[Update])], Long, CantonTimestamp), Option[
        (Offset, Update)
      ], NotUsed] =
        Flow[(Vector[(Offset, Traced[Update])], Long, CantonTimestamp)]
          .map(_._1)
          .mapConcat(identity)
          .map { case (off, tracedUpdate) => (off, tracedUpdate.value) }
          .map(Some(_))

      val updateCheckpointState: Flow[Option[(Offset, Update)], OffsetCheckpoint, NotUsed] =
        Flow[Option[(Offset, Update)]]
          .statefulMap[Option[OffsetCheckpoint], Option[OffsetCheckpoint]](create = () => None)(
            f = {
              // an Offset and Update pair was received
              // update the latest checkpoint
              case (lastOffsetCheckpointO, Some((off, update))) =>
                val domainTimeO = update match {
                  case _: Update.Init => None
                  case _: Update.PartyAddedToParticipant => None
                  case _: Update.PartyAllocationRejected => None
                  case tx: Update.TransactionAccepted => Some((tx.domainId, update.recordTime))
                  case reassignment: Update.ReassignmentAccepted =>
                    reassignment.reassignment match {
                      case _: Reassignment.Unassign =>
                        Some((reassignment.reassignmentInfo.sourceDomain.unwrap, update.recordTime))
                      case _: Reassignment.Assign =>
                        Some((reassignment.reassignmentInfo.targetDomain.unwrap, update.recordTime))
                    }
                  case Update.CommandRejected(recordTime, _, _, domainId, _, _) =>
                    Some((domainId, recordTime))
                  case sim: Update.SequencerIndexMoved => Some((sim.domainId, sim.recordTime))
                  case _: Update.CommitRepair => None
                  case tt: Update.TopologyTransactionEffective => Some((tt.domainId, tt.recordTime))
                }

                val lastDomainTimes = lastOffsetCheckpointO.map(_.domainTimes).getOrElse(Map.empty)
                val newDomainTimes =
                  domainTimeO match {
                    case Some((domainId, recordTime)) =>
                      lastDomainTimes.updated(domainId, recordTime)
                    case None => lastDomainTimes
                  }
                val newOffsetCheckpoint = OffsetCheckpoint(off, newDomainTimes)
                (Some(newOffsetCheckpoint), None)
              // a tick was received, propagate the OffsetCheckpoint
              case (lastOffsetCheckpointO, None) =>
                (lastOffsetCheckpointO, lastOffsetCheckpointO)
            },
            onComplete = _ => None,
          )
          .collect { case Some(oc) => oc }

      val pushCheckpoint: Sink[OffsetCheckpoint, NotUsed] =
        Sink.foreach(updateOffsetCheckpointCache).mapMaterializedValue(_ => NotUsed)

      (tick ~> merge).discard
      broadcast ~> preprocess ~> merge ~> updateCheckpointState ~> pushCheckpoint

      FlowShape(broadcast.in, broadcast.out(1))
    })

}

private[platform] object InMemoryStateUpdater {
  final case class PrepareResult(
      updates: Vector[Traced[TransactionLogUpdate]],
      lastOffset: Offset,
      lastEventSequentialId: Long,
      lastPublicationTime: CantonTimestamp,
      lastTraceContext: TraceContext,
  )
  type UpdaterFlow =
    Boolean => Flow[(Vector[(Offset, Traced[Update])], Long, CantonTimestamp), Vector[
      (Offset, Traced[Update])
    ], NotUsed]
  def owner(
      inMemoryState: InMemoryState,
      prepareUpdatesParallelism: Int,
      preparePackageMetadataTimeOutWarning: FiniteDuration,
      offsetCheckpointCacheUpdateInterval: FiniteDuration,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext): ResourceOwner[UpdaterFlow] = for {
    prepareUpdatesExecutor <- ResourceOwner.forExecutorService(() =>
      InstrumentedExecutors.newWorkStealingExecutor(
        metrics.lapi.threadpool.indexBypass.prepareUpdates,
        prepareUpdatesParallelism,
      )
    )
    updateCachesExecutor <- ResourceOwner.forExecutorService(() =>
      InstrumentedExecutors.newFixedThreadPool(
        metrics.lapi.threadpool.indexBypass.updateInMemoryState,
        1,
      )
    )
    logger = loggerFactory.getTracedLogger(getClass)
  } yield InMemoryStateUpdaterFlow(
    prepareUpdatesParallelism = prepareUpdatesParallelism,
    prepareUpdatesExecutionContext = ExecutionContext.fromExecutorService(prepareUpdatesExecutor),
    updateCachesExecutionContext = ExecutionContext.fromExecutorService(updateCachesExecutor),
    preparePackageMetadataTimeOutWarning = preparePackageMetadataTimeOutWarning,
    offsetCheckpointCacheUpdateInterval = offsetCheckpointCacheUpdateInterval,
    metrics = metrics,
    logger = logger,
  )(
    inMemoryState = inMemoryState,
    prepare = prepare,
    update = update(inMemoryState, logger),
  )

  private[index] def prepare(
      batch: Vector[(Offset, Traced[Update])],
      lastEventSequentialId: Long,
      lastPublicationTime: CantonTimestamp,
  ): PrepareResult = {
    val (offset, traceContext) = batch.lastOption.fold(
      throw new NoSuchElementException("empty batch")
    )(_.bimap(identity, _.traceContext))
    PrepareResult(
      updates = batch.collect {
        case (offset, t @ Traced(u: Update.TransactionAccepted)) =>
          t.map(_ => convertTransactionAccepted(offset, u, t.traceContext))
        case (offset, r @ Traced(u: Update.CommandRejected)) =>
          r.map(_ => convertTransactionRejected(offset, u, r.traceContext))
        case (offset, r @ Traced(u: Update.ReassignmentAccepted)) =>
          r.map(_ => convertReassignmentAccepted(offset, u, r.traceContext))
      },
      lastOffset = offset,
      lastEventSequentialId = lastEventSequentialId,
      lastPublicationTime = lastPublicationTime,
      lastTraceContext = traceContext,
    )
  }

  private[index] def update(
      inMemoryState: InMemoryState,
      logger: TracedLogger,
  )(result: PrepareResult, repairMode: Boolean): Unit = {
    updateCaches(inMemoryState, result.updates, result.lastOffset)
    // must be the last update: see the comment inside the method for more details
    // must be after cache updates: see the comment inside the method for more details
    // in case of Repair Mode we will update directly, at the end from the indexer queue
    if (!repairMode) {
      updateLedgerEnd(
        inMemoryState,
        result.lastOffset,
        result.lastEventSequentialId,
        result.lastPublicationTime,
        logger,
      )(
        result.lastTraceContext
      )
    }
    // must be after LedgerEnd update because this could trigger API actions relating to this LedgerEnd
    // it is expected to be okay to run these in repair mode, as repair operations are not related to tracking
    trackSubmissions(inMemoryState.submissionTracker, result.updates)
    // can be done at any point in the pipeline, it is for debugging only
    trackCommandProgress(inMemoryState.commandProgressTracker, result.updates)
  }

  private def trackSubmissions(
      submissionTracker: SubmissionTracker,
      updates: Vector[Traced[TransactionLogUpdate]],
  ): Unit =
    updates.view
      .collect {
        case Traced(txAccepted: TransactionLogUpdate.TransactionAccepted) =>
          txAccepted.completionStreamResponse

        case Traced(txRejected: TransactionLogUpdate.TransactionRejected) =>
          Some(txRejected.completionStreamResponse)
      }
      .flatten
      .foreach(submissionTracker.onCompletion)

  private def trackCommandProgress(
      commandProgressTracker: CommandProgressTracker,
      updates: Vector[Traced[TransactionLogUpdate]],
  ): Unit =
    updates.view.foreach(commandProgressTracker.processLedgerUpdate)

  private def updateCaches(
      inMemoryState: InMemoryState,
      updates: Vector[Traced[TransactionLogUpdate]],
      lastOffset: Offset,
  ): Unit = {
    updates.foreach { tracedTransaction =>
      tracedTransaction.withTraceContext(implicit traceContext =>
        transaction => {
          inMemoryState.inMemoryFanoutBuffer.push(transaction.offset, tracedTransaction)
          val contractStateEventsBatch = convertToContractStateEvents(transaction)
          NonEmptyVector
            .fromVector(contractStateEventsBatch)
            .foreach(inMemoryState.contractStateCaches.push)
        }
      )
    }
    inMemoryState.cachesUpdatedUpto.set(lastOffset)
  }

  def updateLedgerEnd(
      inMemoryState: InMemoryState,
      lastOffset: Offset,
      lastEventSequentialId: Long,
      lastPublicationTime: CantonTimestamp,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    inMemoryState.ledgerEndCache.set((lastOffset, lastEventSequentialId, lastPublicationTime))
    // the order here is very important: first we need to make data available for point-wise lookups
    // and SQL queries, and only then we can make it available on the streams.
    // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
    inMemoryState.dispatcherState.getDispatcher.signalNewHead(lastOffset)
    logger.debug(s"Updated ledger end at offset $lastOffset - $lastEventSequentialId")
  }

  private[index] def convertLogToStateEvent
      : PartialFunction[TransactionLogUpdate.Event, ContractStateEvent] = {
    case createdEvent: TransactionLogUpdate.CreatedEvent =>
      ContractStateEvent.Created(
        contractId = createdEvent.contractId,
        contract = Contract(
          packageName = createdEvent.packageName,
          packageVersion = createdEvent.packageVersion,
          template = createdEvent.templateId,
          arg = createdEvent.createArgument,
        ),
        globalKey = createdEvent.contractKey.map(k =>
          Key.assertBuild(createdEvent.templateId, k.unversioned, createdEvent.packageName)
        ),
        ledgerEffectiveTime = createdEvent.ledgerEffectiveTime,
        stakeholders = createdEvent.flatEventWitnesses.map(Party.assertFromString),
        eventOffset = createdEvent.eventOffset,
        signatories = createdEvent.createSignatories,
        keyMaintainers = createdEvent.createKeyMaintainers,
        driverMetadata = createdEvent.driverMetadata.toByteArray,
      )
    case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
      ContractStateEvent.Archived(
        contractId = exercisedEvent.contractId,
        globalKey = exercisedEvent.contractKey.map(k =>
          Key.assertBuild(
            exercisedEvent.templateId,
            k.unversioned,
            exercisedEvent.packageName,
          )
        ),
        stakeholders = exercisedEvent.flatEventWitnesses.map(Party.assertFromString),
        eventOffset = exercisedEvent.eventOffset,
      )
  }

  private def convertToContractStateEvents(
      tx: TransactionLogUpdate
  ): Vector[ContractStateEvent] =
    tx match {
      case tx: TransactionLogUpdate.TransactionAccepted =>
        tx.events.iterator.collect(convertLogToStateEvent).toVector
      case _ => Vector.empty
    }

  private def convertTransactionAccepted(
      offset: Offset,
      txAccepted: Update.TransactionAccepted,
      traceContext: TraceContext,
  ): TransactionLogUpdate.TransactionAccepted = {
    val rawEvents =
      TransactionTraversalUtils.preorderTraversalForIngestion(txAccepted.transaction.transaction)

    val blinding = txAccepted.blindingInfo

    val events = rawEvents.collect {
      case (nodeId, create: Create) =>
        TransactionLogUpdate.CreatedEvent(
          eventOffset = offset,
          updateId = txAccepted.updateId,
          nodeIndex = nodeId.index,
          eventSequentialId = 0L,
          eventId = EventId(txAccepted.updateId, nodeId),
          contractId = create.coid,
          ledgerEffectiveTime = txAccepted.transactionMeta.ledgerEffectiveTime,
          templateId = create.templateId,
          packageName = create.packageName,
          packageVersion = create.packageVersion,
          commandId = txAccepted.completionInfoO.map(_.commandId).getOrElse(""),
          workflowId = txAccepted.transactionMeta.workflowId.getOrElse(""),
          contractKey = create.keyOpt.map(k =>
            com.digitalasset.daml.lf.transaction.Versioned(create.version, k.value)
          ),
          treeEventWitnesses = blinding.disclosure.getOrElse(nodeId, Set.empty),
          flatEventWitnesses = create.stakeholders,
          submitters = txAccepted.completionInfoO
            .map(_.actAs.toSet)
            .getOrElse(Set.empty),
          createArgument =
            com.digitalasset.daml.lf.transaction.Versioned(create.version, create.arg),
          createSignatories = create.signatories,
          createObservers = create.stakeholders.diff(create.signatories),
          createKeyHash = create.keyOpt.map(_.globalKey.hash),
          createKey = create.keyOpt.map(_.globalKey),
          createKeyMaintainers = create.keyOpt.map(_.maintainers),
          driverMetadata = txAccepted.contractMetadata
            .get(create.coid)
            .getOrElse(
              throw new IllegalStateException(
                s"missing driver metadata for contract ${create.coid}"
              )
            ),
        )
      case (nodeId, exercise: Exercise) =>
        TransactionLogUpdate.ExercisedEvent(
          eventOffset = offset,
          updateId = txAccepted.updateId,
          nodeIndex = nodeId.index,
          eventSequentialId = 0L,
          eventId = EventId(txAccepted.updateId, nodeId),
          contractId = exercise.targetCoid,
          ledgerEffectiveTime = txAccepted.transactionMeta.ledgerEffectiveTime,
          templateId = exercise.templateId,
          packageName = exercise.packageName,
          commandId = txAccepted.completionInfoO.map(_.commandId).getOrElse(""),
          workflowId = txAccepted.transactionMeta.workflowId.getOrElse(""),
          contractKey = exercise.keyOpt.map(k =>
            com.digitalasset.daml.lf.transaction.Versioned(exercise.version, k.value)
          ),
          treeEventWitnesses = blinding.disclosure.getOrElse(nodeId, Set.empty),
          flatEventWitnesses = if (exercise.consuming) exercise.stakeholders else Set.empty,
          submitters = txAccepted.completionInfoO
            .map(_.actAs.toSet)
            .getOrElse(Set.empty),
          choice = exercise.choiceId,
          actingParties = exercise.actingParties,
          children = exercise.children.iterator
            .map(EventId(txAccepted.updateId, _).toLedgerString)
            .toSeq,
          exerciseArgument = exercise.versionedChosenValue,
          exerciseResult = exercise.versionedExerciseResult,
          consuming = exercise.consuming,
          interfaceId = exercise.interfaceId,
        )
    }

    val completionStreamResponse = txAccepted.completionInfoO
      .map { completionInfo =>
        val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
          deduplicationInfo(completionInfo)

        CompletionFromTransaction.acceptedCompletion(
          submitters = completionInfo.actAs.map(_.toString).toSet,
          recordTime = txAccepted.recordTime,
          offset = offset,
          commandId = completionInfo.commandId,
          updateId = txAccepted.updateId,
          applicationId = completionInfo.applicationId,
          optSubmissionId = completionInfo.submissionId,
          optDeduplicationOffset = deduplicationOffset,
          optDeduplicationDurationSeconds = deduplicationDurationSeconds,
          optDeduplicationDurationNanos = deduplicationDurationNanos,
          domainId = txAccepted.domainId.toProtoPrimitive,
          traceContext = traceContext,
        )
      }

    TransactionLogUpdate.TransactionAccepted(
      updateId = txAccepted.updateId,
      commandId = txAccepted.completionInfoO.map(_.commandId).getOrElse(""),
      workflowId = txAccepted.transactionMeta.workflowId.getOrElse(""),
      effectiveAt = txAccepted.transactionMeta.ledgerEffectiveTime,
      offset = offset,
      events = events.toVector,
      completionStreamResponse = completionStreamResponse,
      domainId = txAccepted.domainId.toProtoPrimitive,
      recordTime = txAccepted.recordTime,
    )
  }

  private def convertTransactionRejected(
      offset: Offset,
      u: Update.CommandRejected,
      traceContext: TraceContext,
  ): TransactionLogUpdate.TransactionRejected = {
    val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
      deduplicationInfo(u.completionInfo)

    TransactionLogUpdate.TransactionRejected(
      offset = offset,
      completionStreamResponse = CompletionFromTransaction.rejectedCompletion(
        submitters = u.completionInfo.actAs.map(_.toString).toSet,
        recordTime = u.recordTime,
        offset = offset,
        commandId = u.completionInfo.commandId,
        status = u.reasonTemplate.status,
        applicationId = u.completionInfo.applicationId,
        optSubmissionId = u.completionInfo.submissionId,
        optDeduplicationOffset = deduplicationOffset,
        optDeduplicationDurationSeconds = deduplicationDurationSeconds,
        optDeduplicationDurationNanos = deduplicationDurationNanos,
        domainId = u.domainId.toProtoPrimitive,
        traceContext = traceContext,
      ),
    )
  }

  private def convertReassignmentAccepted(
      offset: Offset,
      u: Update.ReassignmentAccepted,
      traceContext: TraceContext,
  ): TransactionLogUpdate.ReassignmentAccepted = {
    val completionStreamResponse = u.optCompletionInfo
      .map { completionInfo =>
        val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
          deduplicationInfo(completionInfo)

        CompletionFromTransaction.acceptedCompletion(
          submitters = completionInfo.actAs.map(_.toString).toSet,
          recordTime = u.recordTime,
          offset = offset,
          commandId = completionInfo.commandId,
          updateId = u.updateId,
          applicationId = completionInfo.applicationId,
          optSubmissionId = completionInfo.submissionId,
          optDeduplicationOffset = deduplicationOffset,
          optDeduplicationDurationSeconds = deduplicationDurationSeconds,
          optDeduplicationDurationNanos = deduplicationDurationNanos,
          domainId = u.reassignment match {
            case _: Reassignment.Assign => u.reassignmentInfo.targetDomain.unwrap.toProtoPrimitive
            case _: Reassignment.Unassign =>
              u.reassignmentInfo.sourceDomain.unwrap.toProtoPrimitive
          },
          traceContext = traceContext,
        )
      }

    TransactionLogUpdate.ReassignmentAccepted(
      updateId = u.updateId,
      commandId = u.optCompletionInfo.map(_.commandId).getOrElse(""),
      workflowId = u.workflowId.getOrElse(""),
      offset = offset,
      recordTime = u.recordTime,
      completionStreamResponse = completionStreamResponse,
      reassignmentInfo = u.reassignmentInfo,
      reassignment = u.reassignment match {
        case assign: Reassignment.Assign =>
          val create = assign.createNode
          TransactionLogUpdate.ReassignmentAccepted.Assigned(
            TransactionLogUpdate.CreatedEvent(
              eventOffset = offset,
              updateId = u.updateId,
              nodeIndex = 0, // set 0 for assign-created
              eventSequentialId = 0L,
              eventId = EventId(u.updateId, NodeId(0)), // set 0 for assign-created
              contractId = create.coid,
              ledgerEffectiveTime = assign.ledgerEffectiveTime,
              templateId = create.templateId,
              packageName = create.packageName,
              packageVersion = create.packageVersion,
              commandId = u.optCompletionInfo.map(_.commandId).getOrElse(""),
              workflowId = u.workflowId.getOrElse(""),
              contractKey = create.keyOpt.map(k =>
                com.digitalasset.daml.lf.transaction.Versioned(create.version, k.value)
              ),
              treeEventWitnesses = Set.empty,
              flatEventWitnesses = u.reassignmentInfo.hostedStakeholders.toSet,
              submitters = u.optCompletionInfo
                .map(_.actAs.toSet)
                .getOrElse(Set.empty),
              createArgument =
                com.digitalasset.daml.lf.transaction.Versioned(create.version, create.arg),
              createSignatories = create.signatories,
              createObservers = create.stakeholders.diff(create.signatories),
              createKeyHash = create.keyOpt.map(_.globalKey.hash),
              createKey = create.keyOpt.map(_.globalKey),
              createKeyMaintainers = create.keyOpt.map(_.maintainers),
              driverMetadata = assign.contractMetadata,
            )
          )
        case unassign: Reassignment.Unassign =>
          TransactionLogUpdate.ReassignmentAccepted.Unassigned(unassign)
      },
    )
  }

  private def deduplicationInfo(
      completionInfo: CompletionInfo
  ): (Option[Long], Option[Long], Option[Int]) =
    completionInfo.optDeduplicationPeriod
      .map {
        case DeduplicationOffset(offset) =>
          (Some(offset.toLong), None, None)
        case DeduplicationDuration(duration) =>
          (None, Some(duration.getSeconds), Some(duration.getNano))
      }
      .getOrElse((None, None, None))
}
