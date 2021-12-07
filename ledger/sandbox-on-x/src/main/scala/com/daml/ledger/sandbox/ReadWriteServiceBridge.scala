// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.{
  Configuration,
  LedgerId,
  LedgerInitialConditions,
  LedgerTimeModel,
}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.ledger.participant.state.v2._
import com.daml.ledger.sandbox.ReadWriteServiceBridge.Submission._
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.Transaction.{KeyActive, KeyCreate, NegativeKeyLookup}
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction, GlobalKey, SubmittedTransaction}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{InstrumentedSource, Metrics, Timed}
import com.daml.platform.apiserver.execution.MissingContracts
import com.daml.platform.store.appendonlydao.events._
import com.daml.telemetry.TelemetryContext
import com.google.common.primitives.Longs
import com.google.rpc.code.Code
import com.google.rpc.status.Status

import java.util.UUID
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.collection.Searching
import scala.collection.immutable.VectorMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class ReadWriteServiceBridge(
    participantId: Ref.ParticipantId,
    ledgerId: LedgerId,
    maxDedupSeconds: Int,
    submissionBufferSize: Int,
    contractStore: AtomicReference[Option[ContractStore]],
    metrics: Metrics,
)(implicit
    mat: Materializer,
    loggingContext: LoggingContext,
    servicesExecutionContext: ExecutionContext,
) extends ReadService
    with WriteService
    with AutoCloseable {
  private val offsetIdx = new AtomicLong(0L)

  import ReadWriteServiceBridge._

  private[this] val logger = ContextualizedLogger.get(getClass)

  @volatile private var minQueue = VectorMap.empty[Submission.Transaction, Offset]

  private type KeyInputs = Either[
    com.daml.lf.transaction.Transaction.KeyInputError,
    Map[Key, com.daml.lf.transaction.Transaction.KeyInput],
  ]

  private type SequencerQueue =
    Vector[(Offset, (Submission.Transaction, Map[GlobalKey, Option[ContractId]], Set[ContractId]))]

  private val (conflictCheckingQueue, source) = {
    val parallelCheck = InstrumentedSource
      .queue[Transaction](
        bufferSize = 1024,
        capacityCounter = metrics.daml.SoX.conflictQueueCapacity,
        lengthCounter = metrics.daml.SoX.conflictQueueLength,
        delayTimer = metrics.daml.SoX.conflictQueueDelay,
      )
      .map { tx =>
        val enqueuedAt = contractStore
          .get()
          .getOrElse(throw new RuntimeException("ContractStore not there yet"))
          .cacheOffset()
        minQueue.synchronized {
          minQueue = minQueue.updated(tx, enqueuedAt)
          metrics.daml.SoX.minQueueSizeCounter.inc()
        }
        enqueuedAt -> tx
      }
      .mapAsyncUnordered(64) { case (enqueuedAt, tx) =>
        Timed.future(
          metrics.daml.SoX.parallelConflictCheckingDuration,
          Future {
            (
              tx.transaction.contractKeyInputs,
              tx.transaction.inputContracts,
              tx.transaction.updatedContractKeys,
              tx.transaction.consumedContracts,
            )
          }.flatMap { case (keyInputs, inputContracts, updatedKeys, consumedContracts) =>
            parallelConflictCheck(tx, inputContracts, keyInputs).map(tx =>
              (enqueuedAt, keyInputs, inputContracts, updatedKeys, consumedContracts, tx)
            )
          },
        )
      }

    InstrumentedSource
      .bufferedSource(
        parallelCheck,
        metrics.daml.SoX.queueBeforeSequencer,
        128,
      )
      .statefulMapConcat(sequence)
      .preMaterialize()
  }

  private def sequence: () => (
      (
          Offset,
          KeyInputs,
          Set[ContractId],
          Map[GlobalKey, Option[ContractId]],
          Set[ContractId],
          Either[SoxRejection, Transaction],
      )
  ) => Iterable[
    (Offset, Update)
  ] = () => {
    @volatile var sequencerQueue: SequencerQueue = Vector.empty

    {
      case (_, _, _, _, _, Left(rejection)) =>
        Timed.value(
          metrics.daml.SoX.sequenceDuration, {
            val newIndex = offsetIdx.getAndIncrement()
            val newOffset = toOffset(newIndex)
            minQueue.synchronized {
              minQueue = minQueue.removed(rejection.originalTx)
              metrics.daml.SoX.minQueueSizeCounter.dec()
            }
            Iterable(newOffset -> toRejection(rejection))
          },
        )
      case (
            enqueuedAt,
            keyInputs,
            inputContracts,
            updatedKeys,
            consumedContracts,
            Right(transaction),
          ) =>
        Timed.value(
          metrics.daml.SoX.sequenceDuration, {
            val newIndex = offsetIdx.getAndIncrement()
            val newOffset = toOffset(newIndex)
            checkSequential(
              enqueuedAt,
              keyInputs,
              inputContracts,
              transaction,
              sequencerQueue,
            ) match {
              case Left(rejection) =>
                minQueue.synchronized {
                  minQueue = minQueue.removed(transaction)
                  metrics.daml.SoX.minQueueSizeCounter.dec()
                }
                Iterable(newOffset -> toRejection(rejection))
              case Right(acceptedTx) =>
                minQueue.synchronized {
                  sequencerQueue =
                    sequencerQueue :+ (newOffset -> (transaction, updatedKeys, consumedContracts))
                  val smallestHead = minQueue.head._2
                  val pruneFrom = sequencerQueue.view.map(_._1).search(smallestHead) match {
                    case Searching.Found(foundIndex) => foundIndex - 1
                    case Searching.InsertionPoint(insertionPoint) => insertionPoint - 1
                  }
                  sequencerQueue = sequencerQueue.slice(pruneFrom, sequencerQueue.length)
                  minQueue = minQueue.removed(transaction)
                  metrics.daml.SoX.minQueueSizeCounter.dec()
                  metrics.daml.SoX.sequencerQueueLengthCounter.update(sequencerQueue.size)
                }
                Iterable(newOffset -> successMapper(acceptedTx, newIndex, participantId))
            }
          },
        )
    }
  }

  def invalidInputFromParticipant(
      originalTx: Submission.Transaction
  ): com.daml.lf.transaction.Transaction.KeyInputError => SoxRejection = {
    case com.daml.lf.transaction.Transaction.InconsistentKeys(key) =>
      GenericRejectionFailure(s"Inconsistent keys for $key")(originalTx)
    case com.daml.lf.transaction.Transaction.DuplicateKeys(key) =>
      GenericRejectionFailure(s"Duplicate key for $key")(originalTx)
  }

  private def conflictCheckSlice(
      delta: SequencerQueue,
      keyInputs: KeyInputs,
      inputContracts: Set[ContractId],
      transaction: Submission.Transaction,
  ): Either[SoxRejection, Submission.Transaction] = {
    metrics.daml.SoX.deltaConflictCheckingSize.update(delta.length)
    val (updatedKeys, archives) = collectDelta(delta)

    keyInputs.left
      .map(invalidInputFromParticipant(transaction))
      .flatMap(_.foldLeft[Either[SoxRejection, Unit]](Right(())) {
        case (Right(_), (key, KeyCreate)) =>
          updatedKeys.get(key) match {
            case Some(None) | None => Right(())
            case Some(Some(_)) => Left(DuplicateKey(key)(transaction))
          }
        case (Right(_), (key, NegativeKeyLookup)) =>
          updatedKeys.get(key) match {
            case Some(None) | None => Right(())
            case Some(Some(actual)) =>
              Left(InconsistentContractKey(None, Some(actual))(transaction))
          }
        case (Right(_), (key, KeyActive(cid))) =>
          updatedKeys.get(key) match {
            case Some(Some(`cid`)) | None => Right(())
            case Some(other) => Left(InconsistentContractKey(other, Some(cid))(transaction))
          }
        case (left, _) => left
      })
      .flatMap { _ =>
        val alreadyArchived = inputContracts.intersect(archives)
        if (alreadyArchived.nonEmpty) Left(UnknownContracts(alreadyArchived)(transaction))
        else Right(())
      }
      .map(_ => transaction)
  }

  private def toRejection(rejection: SoxRejection) =
    Update.CommandRejected(
      recordTime = Timestamp.now(),
      completionInfo = rejection.originalTx.submitterInfo.toCompletionInfo,
      reasonTemplate = FinalReason(rejection.toStatus),
    )

  private def checkSequential(
      noConflictUpTo: Offset,
      keyInputs: KeyInputs,
      inputContracts: Set[ContractId],
      transaction: Submission.Transaction,
      sequencerQueue: SequencerQueue,
  ): Either[SoxRejection, Submission.Transaction] =
    Timed.value(
      metrics.daml.SoX.sequentialCheckDuration, {
        val idx = sequencerQueue.view.map(_._1).search(noConflictUpTo) match {
          case Searching.Found(foundIndex) => foundIndex + 1
          case Searching.InsertionPoint(insertionPoint) => insertionPoint
        }

        if (sequencerQueue.size > idx) {
          conflictCheckSlice(
            sequencerQueue.slice(idx, sequencerQueue.length),
            keyInputs,
            inputContracts,
            transaction,
          )
        } else Right(transaction)
      },
    )

  private def parallelConflictCheck(
      transaction: Submission.Transaction,
      inputContracts: Set[ContractId],
      keyInputs: KeyInputs,
  ): Future[Either[SoxRejection, Submission.Transaction]] =
    validateCausalMonotonicity(
      transaction,
      inputContracts,
      transaction.transactionMeta.ledgerEffectiveTime,
      transaction.blindingInfo.divulgence.keySet,
    ).flatMap {
      case Right(_) => validateKeyUsages(transaction, keyInputs)
      case rejection => Future.successful(rejection)
    }.flatMap {
      case Right(_) => validatePartyAllocation(transaction.transaction)
      case rejection => Future.successful(rejection)
    }.map(_.map(_ => transaction))

  private def validatePartyAllocation(
      transaction: SubmittedTransaction
  ): Future[Either[SoxRejection, Unit]] = {
    val _ = transaction
    // TODO implement
    Future.successful(Right(()))
  }

  private def collectDelta(
      delta: SequencerQueue
  ): (Map[Key, Option[ContractId]], Set[ContractId]) = {
    val (updatedKeysBuilder, consumedContractBuilder) = delta.view
      .map(_._2)
      .foldLeft((Map.newBuilder[GlobalKey, Option[ContractId]], Set.newBuilder[ContractId])) {
        case ((updatedKeys, archived), (_, txUpdatedKeys, txConsumedContracts)) =>
          updatedKeys.addAll(txUpdatedKeys) -> archived.addAll(txConsumedContracts)
      }
    updatedKeysBuilder.result() -> consumedContractBuilder.result()
  }

  private def validateCausalMonotonicity(
      transaction: Transaction,
      inputContracts: Set[ContractId],
      transactionLedgerEffectiveTime: Timestamp,
      divulged: Set[ContractId],
  ): Future[Either[SoxRejection, Unit]] = {
    val referredContracts = inputContracts.diff(divulged)
    if (referredContracts.isEmpty) {
      Future.successful(Right(()))
    } else
      contractStore
        .get()
        .getOrElse(throw new RuntimeException("ContractStore not there yet"))
        .lookupMaximumLedgerTime(referredContracts)
        .transform {
          case Failure(MissingContracts(missingContractIds)) =>
            Success(Left(UnknownContracts(missingContractIds)(transaction)))
          case Failure(err) => Success(Left(GenericRejectionFailure(err.getMessage)(transaction)))
          case Success(maximumLedgerEffectiveTime) =>
            maximumLedgerEffectiveTime
              .filter(_ > transactionLedgerEffectiveTime)
              .fold[Try[Either[SoxRejection, Unit]]](Success(Right(())))(
                contractLedgerEffectiveTime =>
                  Success(
                    Left(
                      CausalMonotonicityViolation(
                        contractLedgerEffectiveTime = contractLedgerEffectiveTime,
                        transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
                      )(transaction)
                    )
                  )
              )
        }
  }

  override def isApiDeduplicationEnabled: Boolean = true

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    toSubmissionResult(
      conflictCheckingQueue.offer(
        Submission.Transaction(
          submitterInfo = submitterInfo,
          transactionMeta = transactionMeta,
          transaction = transaction,
          estimatedInterpretationCost = estimatedInterpretationCost,
          blindingInfo = Blinding.blind(transaction),
        )
      )
    )

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: Ref.SubmissionId,
      config: Configuration,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    submit(
      Submission.Config(
        maxRecordTime = maxRecordTime,
        submissionId = submissionId,
        config = config,
      )
    )

  override def currentHealth(): HealthStatus = Healthy

  override def allocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    submit(
      Submission.AllocateParty(
        hint = hint,
        displayName = displayName,
        submissionId = submissionId,
      )
    )

  override def uploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[Archive],
      sourceDescription: Option[String],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    submit(
      Submission.UploadPackages(
        submissionId = submissionId,
        archives = archives,
        sourceDescription = sourceDescription,
      )
    )

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult] =
    CompletableFuture.completedFuture(
      PruningResult.ParticipantPruned
    )

  override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(
      LedgerInitialConditions(
        ledgerId = ledgerId,
        config = Configuration(
          generation = 1L,
          timeModel = LedgerTimeModel.reasonableDefault,
          maxDeduplicationTime = java.time.Duration.ofSeconds(maxDedupSeconds.toLong),
        ),
        initialRecordTime = Timestamp.now(),
      )
    )

  var stateUpdatesWasCalledAlready = false
  override def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit loggingContext: LoggingContext): Source[(Offset, Update), NotUsed] = {
    // TODO for PoC purposes:
    //   This method may only be called once, either with `beginAfter` set or unset.
    //   A second call will result in an error unless the server is restarted.
    //   Bootstrapping the bridge from indexer persistence is supported.
    synchronized {
      if (stateUpdatesWasCalledAlready)
        throw new IllegalStateException("not allowed to call this twice")
      else stateUpdatesWasCalledAlready = true
    }
    logger.info("Indexer subscribed to state updates.")
    beginAfter.foreach(offset =>
      logger.warn(
        s"Indexer subscribed from a specific offset $offset. This offset is not taking into consideration, and does not change the behavior of the ReadWriteServiceBridge. Only valid use case supported: service starting from an already ingested database, and indexer subscribes from exactly the ledger-end."
      )
    )

    queueSource.merge(source)
  }

  val (queue: BoundedSourceQueue[Submission], queueSource: Source[(Offset, Update), NotUsed]) =
    Source
      .queue[Submission](submissionBufferSize)
      .map { submission =>
        val newOffsetIdx = offsetIdx.getAndIncrement()
        val newOffset = toOffset(newOffsetIdx)
        (newOffset, successMapper(submission, newOffsetIdx, participantId))
      }
      .preMaterialize()

  logger.info(
    s"BridgeLedgerFactory initialized. Configuration: [maxDedupSeconds: $maxDedupSeconds, submissionBufferSize: $submissionBufferSize]"
  )

  private def submit(submission: Submission): CompletionStage[SubmissionResult] =
    toSubmissionResult(queue.offer(submission))

  private def validateKeyUsages(
      transaction: Transaction,
      keyInputs: KeyInputs,
  ): Future[Either[SoxRejection, Unit]] = {
    val readers = transaction.transaction.informees // Is it informees??
    keyInputs.fold(
      err => Future.successful(Left(GenericRejectionFailure(err.toString)(transaction))),
      _.foldLeft(Future.successful[Either[SoxRejection, Unit]](Right(()))) {
        case (f, (key, inputState)) =>
          f.flatMap {
            case Right(_) =>
              contractStore
                .get()
                .getOrElse(throw new RuntimeException("ContractStore not there yet"))
                .lookupContractKey(readers, key)
                .map {
                  lookupResult =>
                    (inputState, lookupResult) match {
                      case (NegativeKeyLookup, Some(actual)) =>
                        Left(InconsistentContractKey(None, Some(actual))(transaction))
                      case (KeyCreate, Some(_)) =>
                        Left(DuplicateKey(key)(transaction))
                      case (KeyActive(expected), actual) if !actual.contains(expected) =>
                        Left(InconsistentContractKey(Some(expected), actual)(transaction))
                      case _ => Right(())
                    }
                }
            case left => Future.successful(left)
          }
      },
    )
  }

  override def close(): Unit = {
    logger.info("Shutting down BridgeLedgerFactory.")
    queue.complete()
  }
}

object ReadWriteServiceBridge {
  private implicit val errorLoggingContext: ContextualizedErrorLogger = NoLogging
  trait Submission
  object Submission {
    sealed trait SoxRejection extends Submission {
      def toStatus: Status

      val originalTx: Submission.Transaction
    }
    final case class DuplicateKey(key: GlobalKey)(override val originalTx: Transaction)
        extends SoxRejection {
      override def toStatus: Status =
        LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
          .RejectWithContractKeyArg("DuplicateKey: contract key is not unique", key)
          .rpcStatus(None)
    }
    final case class InconsistentContractKey(
        expectation: Option[ContractId],
        result: Option[ContractId],
    )(override val originalTx: Transaction)
        extends SoxRejection {
      override def toStatus: Status =
        LedgerApiErrors.ConsistencyErrors.InconsistentContractKey
          .Reject(
            s"Contract key lookup with different results. Expected: $expectation, result: $result"
          )
          .rpcStatus(None)
    }

    final case class GenericRejectionFailure(details: String)(override val originalTx: Transaction)
        extends SoxRejection {
      override def toStatus: Status =
        // TODO wrong error
        LedgerApiErrors.InternalError.VersionService(details).rpcStatus(None)
    }

    final case class CausalMonotonicityViolation(
        contractLedgerEffectiveTime: Timestamp,
        transactionLedgerEffectiveTime: Timestamp,
    )(override val originalTx: Transaction)
        extends SoxRejection {
      override def toStatus: Status = LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
        .RejectSimple("ADD DETAILS FOR LET failure")
        .rpcStatus(None)
    }

    final case class UnknownContracts(ids: Set[ContractId])(override val originalTx: Transaction)
        extends SoxRejection {
      override def toStatus: Status = LedgerApiErrors.ConsistencyErrors.ContractNotFound
        .MultipleContractsNotFound(ids.map(_.coid))
        .rpcStatus(None)
    }

    case class Transaction(
        submitterInfo: SubmitterInfo,
        transactionMeta: TransactionMeta,
        transaction: SubmittedTransaction,
        estimatedInterpretationCost: Long,
        blindingInfo: BlindingInfo,
    ) extends Submission
    case class Config(
        maxRecordTime: Time.Timestamp,
        submissionId: Ref.SubmissionId,
        config: Configuration,
    ) extends Submission
    case class AllocateParty(
        hint: Option[Ref.Party],
        displayName: Option[String],
        submissionId: Ref.SubmissionId,
    ) extends Submission

    case class UploadPackages(
        submissionId: Ref.SubmissionId,
        archives: List[Archive],
        sourceDescription: Option[String],
    ) extends Submission

    case class Rejection(rejection: SoxRejection) extends Submission
  }

  private[this] val logger = ContextualizedLogger.get(getClass)

  def successMapper(submission: Submission, index: Long, participantId: Ref.ParticipantId): Update =
    submission match {
      case s: Submission.AllocateParty =>
        val party = s.hint.getOrElse(UUID.randomUUID().toString)
        Update.PartyAddedToParticipant(
          party = Ref.Party.assertFromString(party),
          displayName = s.displayName.getOrElse(party),
          participantId = participantId,
          recordTime = Time.Timestamp.now(),
          submissionId = Some(s.submissionId),
        )

      case s: Submission.Config =>
        Update.ConfigurationChanged(
          recordTime = Time.Timestamp.now(),
          submissionId = s.submissionId,
          participantId = participantId,
          newConfiguration = s.config,
        )

      case s: Submission.UploadPackages =>
        Update.PublicPackageUpload(
          archives = s.archives,
          sourceDescription = s.sourceDescription,
          recordTime = Time.Timestamp.now(),
          submissionId = Some(s.submissionId),
        )

      case s: Submission.Transaction =>
        Update.TransactionAccepted(
          optCompletionInfo = Some(s.submitterInfo.toCompletionInfo),
          transactionMeta = s.transactionMeta,
          transaction = s.transaction.asInstanceOf[CommittedTransaction],
          transactionId = Ref.TransactionId.assertFromString(index.toString),
          recordTime = Time.Timestamp.now(),
          divulgedContracts = Nil,
          blindingInfo = None,
        )
    }

  def toOffset(index: Long): Offset = Offset.fromByteArray(Longs.toByteArray(index))

  def toSubmissionResult(
      queueOfferResult: QueueOfferResult
  )(implicit loggingContext: LoggingContext): CompletableFuture[SubmissionResult] =
    CompletableFuture.completedFuture(
      queueOfferResult match {
        case QueueOfferResult.Enqueued => SubmissionResult.Acknowledged
        case QueueOfferResult.Dropped =>
          logger.warn(
            "Buffer overflow: new submission is not added, signalized `Overloaded` for caller."
          )
          SubmissionResult.SynchronousError(
            Status(
              Code.RESOURCE_EXHAUSTED.value
            )
          )
        case QueueOfferResult.Failure(throwable) =>
          logger.error("Error enqueueing new submission.", throwable)
          SubmissionResult.SynchronousError(
            Status(
              Code.INTERNAL.value,
              throwable.getMessage,
            )
          )
        case QueueOfferResult.QueueClosed =>
          logger.error("Error enqueueing new submission: queue is closed.")
          SubmissionResult.SynchronousError(
            Status(
              Code.INTERNAL.value,
              "Queue is closed",
            )
          )
      }
    )
}
