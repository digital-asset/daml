// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import cats.implicits.{catsSyntaxSemigroup, toBifunctorOps}
import com.daml.daml_lf_dev.DamlLf
import com.daml.executors.InstrumentedExecutors
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref.HexString
import com.daml.lf.data.Time
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.Node.{Create, Exercise}
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{Node, NodeId, Util}
import com.daml.metrics.Timed
import com.daml.timer.FutureCheck.*
import com.digitalasset.canton.ledger.api.DeduplicationPeriod.{
  DeduplicationDuration,
  DeduplicationOffset,
}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.{CompletionInfo, Reassignment, Update}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.index.InMemoryStateUpdater.{PrepareResult, UpdaterFlow}
import com.digitalasset.canton.platform.store.CompletionFromTransaction
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.CompletionDetails
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.Implicits.packageMetadataSemigroup
import com.digitalasset.canton.platform.{Contract, InMemoryState, Key, Party}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/** Builder of the in-memory state updater Pekko flow.
  *
  * This flow is attached at the end of the Indexer pipeline,
  * consumes the [[com.digitalasset.canton.ledger.participant.state.v2.Update]]s (that have been ingested by the Indexer
  * into the Index database) for populating the Ledger API server in-memory state (see [[InMemoryState]]).
  */
private[platform] object InMemoryStateUpdaterFlow {

  private[index] def apply(
      prepareUpdatesParallelism: Int,
      prepareUpdatesExecutionContext: ExecutionContext,
      updateCachesExecutionContext: ExecutionContext,
      preparePackageMetadataTimeOutWarning: FiniteDuration,
      metrics: Metrics,
      logger: TracedLogger,
  )(
      prepare: (Vector[(Offset, Traced[Update])], Long) => PrepareResult,
      update: PrepareResult => Unit,
  )(implicit traceContext: TraceContext): UpdaterFlow =
    Flow[(Vector[(Offset, Traced[Update])], Long)]
      .filter(_._1.nonEmpty)
      .mapAsync(prepareUpdatesParallelism) { case (batch, lastEventSequentialId) =>
        Future {
          prepare(batch, lastEventSequentialId)
        }(prepareUpdatesExecutionContext)
          .checkIfComplete(preparePackageMetadataTimeOutWarning)(
            logger.warn(
              s"Package Metadata View live update did not finish in ${preparePackageMetadataTimeOutWarning.toMillis}ms"
            )
          )
      }
      .async
      .mapAsync(1) { result =>
        Future {
          update(result)
          metrics.daml.index.ledgerEndSequentialId.updateValue(result.lastEventSequentialId)
        }(updateCachesExecutionContext)
      }
}

private[platform] object InMemoryStateUpdater {
  final case class PrepareResult(
      updates: Vector[Traced[TransactionLogUpdate]],
      lastOffset: Offset,
      lastEventSequentialId: Long,
      lastTraceContext: TraceContext,
      packageMetadata: PackageMetadata,
  )
  type UpdaterFlow = Flow[(Vector[(Offset, Traced[Update])], Long), Unit, NotUsed]
  def owner(
      inMemoryState: InMemoryState,
      prepareUpdatesParallelism: Int,
      preparePackageMetadataTimeOutWarning: FiniteDuration,
      multiDomainEnabled: Boolean,
      metrics: Metrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext): ResourceOwner[UpdaterFlow] = for {
    prepareUpdatesExecutor <- ResourceOwner.forExecutorService(() =>
      InstrumentedExecutors.newWorkStealingExecutor(
        metrics.daml.lapi.threadpool.indexBypass.prepareUpdates,
        prepareUpdatesParallelism,
      )
    )
    updateCachesExecutor <- ResourceOwner.forExecutorService(() =>
      InstrumentedExecutors.newFixedThreadPool(
        metrics.daml.lapi.threadpool.indexBypass.updateInMemoryState,
        1,
      )
    )
    logger = loggerFactory.getTracedLogger(getClass)
  } yield InMemoryStateUpdaterFlow(
    prepareUpdatesParallelism = prepareUpdatesParallelism,
    prepareUpdatesExecutionContext = ExecutionContext.fromExecutorService(prepareUpdatesExecutor),
    updateCachesExecutionContext = ExecutionContext.fromExecutorService(updateCachesExecutor),
    preparePackageMetadataTimeOutWarning = preparePackageMetadataTimeOutWarning,
    metrics = metrics,
    logger = logger,
  )(
    prepare = prepare(
      archiveToMetadata = (archive, timestamp) =>
        Timed.value(
          metrics.daml.index.packageMetadata.decodeArchive,
          PackageMetadata.from(archive, timestamp),
        ),
      multiDomainEnabled = multiDomainEnabled,
    ),
    update = update(inMemoryState, logger),
  )

  private[index] def extractMetadataFromUploadedPackages(
      archiveToMetadata: (DamlLf.Archive, Time.Timestamp) => PackageMetadata
  )(
      batch: Vector[(Offset, Traced[Update])]
  ): PackageMetadata =
    batch.view
      .collect { case (_, Traced(packageUpload: Update.PublicPackageUpload)) => packageUpload }
      .foldLeft(PackageMetadata()) { case (pkgMeta, packageUpload) =>
        packageUpload.archives.view
          .map(archiveToMetadata(_, packageUpload.recordTime))
          .foldLeft(pkgMeta)(_ |+| _)
      }

  private[index] def prepare(
      archiveToMetadata: (DamlLf.Archive, Time.Timestamp) => PackageMetadata,
      multiDomainEnabled: Boolean,
  )(
      batch: Vector[(Offset, Traced[Update])],
      lastEventSequentialId: Long,
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
        case (offset, r @ Traced(u: Update.ReassignmentAccepted)) if multiDomainEnabled =>
          r.map(_ => convertReassignmentAccepted(offset, u, r.traceContext))
      },
      lastOffset = offset,
      lastEventSequentialId = lastEventSequentialId,
      lastTraceContext = traceContext,
      packageMetadata = extractMetadataFromUploadedPackages(archiveToMetadata)(batch),
    )
  }

  private[index] def update(
      inMemoryState: InMemoryState,
      logger: TracedLogger,
  )(result: PrepareResult): Unit = {
    inMemoryState.packageMetadataView.update(result.packageMetadata)
    updateCaches(inMemoryState, result.updates)
    // must be the last update: see the comment inside the method for more details
    // must be after cache updates: see the comment inside the method for more details
    updateLedgerEnd(inMemoryState, result.lastOffset, result.lastEventSequentialId, logger)(
      result.lastTraceContext
    )
    // must be after LedgerEnd update because this could trigger API actions relating to this LedgerEnd
    trackSubmissions(inMemoryState.submissionTracker, result.updates)
  }

  private def trackSubmissions(
      submissionTracker: SubmissionTracker,
      updates: Vector[Traced[TransactionLogUpdate]],
  ): Unit =
    updates.view
      .collect {
        case Traced(
              TransactionLogUpdate.TransactionAccepted(_, _, _, _, _, _, Some(completionDetails), _)
            ) =>
          completionDetails.completionStreamResponse -> completionDetails.submitters
        case Traced(rejected: TransactionLogUpdate.TransactionRejected) =>
          rejected.completionDetails.completionStreamResponse -> rejected.completionDetails.submitters
      }
      .foreach(submissionTracker.onCompletion)

  private def updateCaches(
      inMemoryState: InMemoryState,
      updates: Vector[Traced[TransactionLogUpdate]],
  ): Unit =
    updates.foreach { tracedTransaction =>
      // TODO(i12283) LLP: Batch update caches
      tracedTransaction.withTraceContext(implicit traceContext =>
        transaction => {
          inMemoryState.inMemoryFanoutBuffer.push(transaction.offset, tracedTransaction)
          val contractStateEventsBatch = convertToContractStateEvents(transaction)
          if (contractStateEventsBatch.nonEmpty) {
            inMemoryState.contractStateCaches.push(contractStateEventsBatch)
          }
        }
      )
    }

  private def updateLedgerEnd(
      inMemoryState: InMemoryState,
      lastOffset: Offset,
      lastEventSequentialId: Long,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    inMemoryState.ledgerEndCache.set((lastOffset, lastEventSequentialId))
    // the order here is very important: first we need to make data available for point-wise lookups
    // and SQL queries, and only then we can make it available on the streams.
    // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
    inMemoryState.dispatcherState.getDispatcher.signalNewHead(lastOffset)
    logger.debug(s"Updated ledger end at offset $lastOffset - $lastEventSequentialId")
  }

  private def convertToContractStateEvents(
      tx: TransactionLogUpdate
  ): Vector[ContractStateEvent] =
    tx match {
      case tx: TransactionLogUpdate.TransactionAccepted =>
        tx.events.iterator.collect {
          case createdEvent: TransactionLogUpdate.CreatedEvent =>
            ContractStateEvent.Created(
              contractId = createdEvent.contractId,
              contract = Contract(
                template = createdEvent.templateId,
                arg = createdEvent.createArgument,
              ),
              globalKey = createdEvent.contractKey.map(k =>
                Key.assertBuild(createdEvent.templateId, k.unversioned, Util.sharedKey(k.version))
              ),
              ledgerEffectiveTime = createdEvent.ledgerEffectiveTime,
              stakeholders = createdEvent.flatEventWitnesses.map(Party.assertFromString),
              eventOffset = createdEvent.eventOffset,
              eventSequentialId = createdEvent.eventSequentialId,
              agreementText = createdEvent.createAgreementText,
              signatories = createdEvent.createSignatories,
              keyMaintainers = createdEvent.createKeyMaintainers,
              driverMetadata = createdEvent.driverMetadata.map(_.toByteArray),
            )
          case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
            ContractStateEvent.Archived(
              contractId = exercisedEvent.contractId,
              globalKey = exercisedEvent.contractKey.map(k =>
                Key.assertBuild(exercisedEvent.templateId, k.unversioned, Util.sharedKey(k.version))
              ),
              stakeholders = exercisedEvent.flatEventWitnesses.map(Party.assertFromString),
              eventOffset = exercisedEvent.eventOffset,
              eventSequentialId = exercisedEvent.eventSequentialId,
            )
        }.toVector
      case _ => Vector.empty
    }

  private def convertTransactionAccepted(
      offset: Offset,
      txAccepted: Update.TransactionAccepted,
      traceContext: TraceContext,
  ): TransactionLogUpdate.TransactionAccepted = {
    // TODO(i12283) LLP: Extract in common functionality together with duplicated code in [[UpdateToDbDto]]
    val rawEvents = txAccepted.transaction.transaction
      .foldInExecutionOrder(List.empty[(NodeId, Node)])(
        exerciseBegin = (acc, nid, node) => ((nid -> node) :: acc, ChildrenRecursion.DoRecurse),
        // Rollback nodes are not indexed
        rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
        leaf = (acc, nid, node) => (nid -> node) :: acc,
        exerciseEnd = (acc, _, _) => acc,
        rollbackEnd = (acc, _, _) => acc,
      )
      .reverseIterator

    // TODO(i12283) LLP: Deduplicate blinding info computation with the work done in [[UpdateToDbDto]]
    val blinding = txAccepted.blindingInfoO.getOrElse(Blinding.blind(txAccepted.transaction))

    val events = rawEvents.collect {
      case (nodeId, create: Create) =>
        TransactionLogUpdate.CreatedEvent(
          eventOffset = offset,
          transactionId = txAccepted.transactionId,
          nodeIndex = nodeId.index,
          eventSequentialId = 0L,
          eventId = EventId(txAccepted.transactionId, nodeId),
          contractId = create.coid,
          ledgerEffectiveTime = txAccepted.transactionMeta.ledgerEffectiveTime,
          templateId = create.templateId,
          commandId = txAccepted.completionInfoO.map(_.commandId).getOrElse(""),
          workflowId = txAccepted.transactionMeta.workflowId.getOrElse(""),
          contractKey =
            create.keyOpt.map(k => com.daml.lf.transaction.Versioned(create.version, k.value)),
          treeEventWitnesses = blinding.disclosure.getOrElse(nodeId, Set.empty),
          flatEventWitnesses = create.stakeholders,
          submitters = txAccepted.completionInfoO
            .map(_.actAs.toSet)
            .getOrElse(Set.empty),
          createArgument = com.daml.lf.transaction.Versioned(create.version, create.arg),
          createSignatories = create.signatories,
          createObservers = create.stakeholders.diff(create.signatories),
          createAgreementText = Some(create.agreementText).filter(_.nonEmpty),
          createKeyHash = create.keyOpt.map(_.globalKey.hash),
          createKey = create.keyOpt.map(_.globalKey),
          createKeyMaintainers = create.keyOpt.map(_.maintainers),
          driverMetadata = txAccepted.contractMetadata.get(create.coid),
        )
      case (nodeId, exercise: Exercise) =>
        TransactionLogUpdate.ExercisedEvent(
          eventOffset = offset,
          transactionId = txAccepted.transactionId,
          nodeIndex = nodeId.index,
          eventSequentialId = 0L,
          eventId = EventId(txAccepted.transactionId, nodeId),
          contractId = exercise.targetCoid,
          ledgerEffectiveTime = txAccepted.transactionMeta.ledgerEffectiveTime,
          templateId = exercise.templateId,
          commandId = txAccepted.completionInfoO.map(_.commandId).getOrElse(""),
          workflowId = txAccepted.transactionMeta.workflowId.getOrElse(""),
          contractKey =
            exercise.keyOpt.map(k => com.daml.lf.transaction.Versioned(exercise.version, k.value)),
          treeEventWitnesses = blinding.disclosure.getOrElse(nodeId, Set.empty),
          flatEventWitnesses = if (exercise.consuming) exercise.stakeholders else Set.empty,
          submitters = txAccepted.completionInfoO
            .map(_.actAs.toSet)
            .getOrElse(Set.empty),
          choice = exercise.choiceId,
          actingParties = exercise.actingParties,
          children = exercise.children.iterator
            .map(EventId(txAccepted.transactionId, _).toLedgerString)
            .toSeq,
          exerciseArgument = exercise.versionedChosenValue,
          exerciseResult = exercise.versionedExerciseResult,
          consuming = exercise.consuming,
          interfaceId = exercise.interfaceId,
        )
    }

    val completionDetails = txAccepted.completionInfoO
      .map { completionInfo =>
        val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
          deduplicationInfo(completionInfo)

        CompletionDetails(
          CompletionFromTransaction.acceptedCompletion(
            recordTime = txAccepted.recordTime,
            offset = offset,
            commandId = completionInfo.commandId,
            transactionId = txAccepted.transactionId,
            applicationId = completionInfo.applicationId,
            optSubmissionId = completionInfo.submissionId,
            optDeduplicationOffset = deduplicationOffset,
            optDeduplicationDurationSeconds = deduplicationDurationSeconds,
            optDeduplicationDurationNanos = deduplicationDurationNanos,
            domainId = Some(txAccepted.domainId.toProtoPrimitive), // TODO(i15280)
            traceContext = traceContext,
          ),
          submitters = completionInfo.actAs.toSet,
        )
      }

    TransactionLogUpdate.TransactionAccepted(
      transactionId = txAccepted.transactionId,
      commandId = txAccepted.completionInfoO.map(_.commandId).getOrElse(""),
      workflowId = txAccepted.transactionMeta.workflowId.getOrElse(""),
      effectiveAt = txAccepted.transactionMeta.ledgerEffectiveTime,
      offset = offset,
      events = events.toVector,
      completionDetails = completionDetails,
      domainId = Some(txAccepted.domainId.toProtoPrimitive), // TODO(i15280)
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
      completionDetails = CompletionDetails(
        CompletionFromTransaction.rejectedCompletion(
          recordTime = u.recordTime,
          offset = offset,
          commandId = u.completionInfo.commandId,
          status = u.reasonTemplate.status,
          applicationId = u.completionInfo.applicationId,
          optSubmissionId = u.completionInfo.submissionId,
          optDeduplicationOffset = deduplicationOffset,
          optDeduplicationDurationSeconds = deduplicationDurationSeconds,
          optDeduplicationDurationNanos = deduplicationDurationNanos,
          domainId = Some(u.domainId.toProtoPrimitive), // TODO(i15280)
          traceContext = traceContext,
        ),
        submitters = u.completionInfo.actAs.toSet,
      ),
    )
  }

  private def convertReassignmentAccepted(
      offset: Offset,
      u: Update.ReassignmentAccepted,
      traceContext: TraceContext,
  ): TransactionLogUpdate.ReassignmentAccepted = {
    val completionDetails = u.optCompletionInfo
      .map { completionInfo =>
        val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
          deduplicationInfo(completionInfo)

        CompletionDetails(
          CompletionFromTransaction.acceptedCompletion(
            recordTime = u.recordTime,
            offset = offset,
            commandId = completionInfo.commandId,
            transactionId = u.updateId,
            applicationId = completionInfo.applicationId,
            optSubmissionId = completionInfo.submissionId,
            optDeduplicationOffset = deduplicationOffset,
            optDeduplicationDurationSeconds = deduplicationDurationSeconds,
            optDeduplicationDurationNanos = deduplicationDurationNanos,
            domainId = Some(u.reassignment match {
              case _: Reassignment.Assign => u.reassignmentInfo.targetDomain.unwrap.toProtoPrimitive
              case _: Reassignment.Unassign =>
                u.reassignmentInfo.sourceDomain.unwrap.toProtoPrimitive
            }),
            traceContext = traceContext,
          ),
          submitters = completionInfo.actAs.toSet,
        )
      }

    TransactionLogUpdate.ReassignmentAccepted(
      updateId = u.updateId,
      commandId = u.optCompletionInfo.map(_.commandId).getOrElse(""),
      workflowId = u.workflowId.getOrElse(""),
      offset = offset,
      completionDetails = completionDetails,
      reassignmentInfo = u.reassignmentInfo,
      reassignment = u.reassignment match {
        case assign: Reassignment.Assign =>
          val create = assign.createNode
          TransactionLogUpdate.ReassignmentAccepted.Assigned(
            TransactionLogUpdate.CreatedEvent(
              eventOffset = offset,
              transactionId = u.updateId,
              nodeIndex = 0, // set 0 for assign-created
              eventSequentialId = 0L,
              eventId = EventId(u.updateId, NodeId(0)), // set 0 for assign-created
              contractId = create.coid,
              ledgerEffectiveTime = assign.ledgerEffectiveTime,
              templateId = create.templateId,
              commandId = u.optCompletionInfo.map(_.commandId).getOrElse(""),
              workflowId = u.workflowId.getOrElse(""),
              contractKey =
                create.keyOpt.map(k => com.daml.lf.transaction.Versioned(create.version, k.value)),
              treeEventWitnesses = Set.empty,
              flatEventWitnesses = u.reassignmentInfo.hostedStakeholders.toSet,
              submitters = u.optCompletionInfo
                .map(_.actAs.toSet)
                .getOrElse(Set.empty),
              createArgument = com.daml.lf.transaction.Versioned(create.version, create.arg),
              createSignatories = create.signatories,
              createObservers = create.stakeholders.diff(create.signatories),
              createAgreementText = Some(create.agreementText).filter(_.nonEmpty),
              createKeyHash = create.keyOpt.map(_.globalKey.hash),
              createKey = create.keyOpt.map(_.globalKey),
              createKeyMaintainers = create.keyOpt.map(_.maintainers),
              driverMetadata = Some(assign.contractMetadata),
            )
          )
        case unassign: Reassignment.Unassign =>
          TransactionLogUpdate.ReassignmentAccepted.Unassigned(unassign)
      },
    )
  }

  private def deduplicationInfo(
      completionInfo: CompletionInfo
  ): (Option[HexString], Option[Long], Option[Int]) =
    completionInfo.optDeduplicationPeriod
      .map {
        case DeduplicationOffset(offset) =>
          (Some(offset.toHexString), None, None)
        case DeduplicationDuration(duration) =>
          (None, Some(duration.getSeconds), Some(duration.getNano))
      }
      .getOrElse((None, None, None))
}
