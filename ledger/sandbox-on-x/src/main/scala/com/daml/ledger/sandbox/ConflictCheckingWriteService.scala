// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.{Configuration, LedgerId}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.ledger.participant.state.v2._
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.sandbox.ConflictCheckingWriteService.Submission._
import com.daml.ledger.sandbox.ConflictCheckingWriteService.fromOffset
import com.daml.ledger.sandbox.SoxRejection._
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.Transaction.{
  KeyActive,
  KeyCreate,
  NegativeKeyLookup,
  KeyInput => TxKeyInput,
}
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction, SubmittedTransaction}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{InstrumentedSource, Metrics, Timed}
import com.daml.platform.ApiOffset
import com.daml.platform.apiserver.execution.MissingContracts
import com.daml.platform.store.appendonlydao.events
import com.daml.platform.store.appendonlydao.events._
import com.daml.telemetry.TelemetryContext
import com.google.common.primitives.Longs
import com.google.rpc.code.Code
import com.google.rpc.status.Status

import java.util.UUID
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class ConflictCheckingWriteService(
    readServiceWithFeedSubscriber: ReadServiceWithFeedSubscriber,
    participantId: Ref.ParticipantId,
    ledgerId: LedgerId,
    maxDedupSeconds: Int,
    submissionBufferSize: Int,
    indexService: IndexService,
    metrics: Metrics,
    // TODO SoX: Wire up in config
    implicitPartyAllocation: Boolean = false,
)(implicit
    mat: Materializer,
    loggingContext: LoggingContext,
    servicesExecutionContext: ExecutionContext,
) extends WriteService
    with AutoCloseable {
  private lazy val offsetIdx = new AtomicLong(
    Await.result(
      // PoC warning: Don't take this seriously
      indexService
        .currentLedgerEnd()
        .map(o => fromOffset(ApiOffset.assertFromString(o.value))),
      10.seconds,
    )
  )

  // Note: ledger entries are written in batches, and this variable is updated while writing a ledger configuration
  // changed entry. Transactions written around the same time as a configuration change entry might not use the correct
  // time model.
  private[this] lazy val currentConfiguration =
    new AtomicReference[Option[Configuration]](
      // PoC warning: Don't take this seriously
      Await.result(indexService.lookupConfiguration(), 10.seconds).map(_._2)
    )

  import ConflictCheckingWriteService._

  private[this] val logger = ContextualizedLogger.get(getClass)

  private type KeyInputs = Map[Key, TxKeyInput]
  private type TransactionEffects = (
      Map[Key, TxKeyInput],
      Set[events.ContractId],
      Map[Key, Option[events.ContractId]],
      Set[events.ContractId],
  )

  private val (conflictCheckingQueue, source) =
    InstrumentedSource
      .queue[Transaction](
        bufferSize = 1024,
        capacityCounter = metrics.daml.SoX.conflictQueueCapacity,
        lengthCounter = metrics.daml.SoX.conflictQueueLength,
        delayTimer = metrics.daml.SoX.conflictQueueDelay,
      )
      .mapAsyncUnordered(64) { tx =>
        precomputeTransactionOutputs(tx).map(_.map((tx, _)))
      }
      .mapAsync(1) {
        case Left(rejection) => Future.successful(Left(rejection))
        case Right((tx, transactionEffects)) =>
          indexService
            .currentLedgerEnd()
            .map(ledgerEnd =>
              Right((ApiOffset.assertFromString(ledgerEnd.value), tx, transactionEffects))
            )
      }
      .mapAsync(64) {
        case Left(rejection) => Future.successful(Left(rejection))
        case Right((noConflictUpTo, tx, transactionEffects @ (keyInputs, inputContracts, _, _))) =>
          conflictCheckWithCommitted(tx, inputContracts, keyInputs)
            .map(_.map(_ => (noConflictUpTo, tx, transactionEffects)))
      }
      .statefulMapConcat(conflictCheckWithDelta)
      .preMaterialize()

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

  val (queue: BoundedSourceQueue[Submission], queueSource: Source[(Offset, Update), NotUsed]) =
    Source
      .queue[Submission](submissionBufferSize)
      .map { submission =>
        val newOffsetIdx = offsetIdx.incrementAndGet()
        val newOffset = toOffset(newOffsetIdx)
        (newOffset, successMapper(submission, newOffsetIdx, participantId))
      }
      .preMaterialize()

  logger.info(
    s"BridgeLedgerFactory initialized. Configuration: [maxDedupSeconds: $maxDedupSeconds, submissionBufferSize: $submissionBufferSize]"
  )

  private def submit(submission: Submission): CompletionStage[SubmissionResult] =
    toSubmissionResult(queue.offer(submission))

  private def precomputeTransactionOutputs(
      transaction: Transaction
  ): Future[Either[SoxRejection, TransactionEffects]] = {
    lazy val contractKeyInputs =
      transaction.transaction.contractKeyInputs.left.map(invalidInputFromParticipant(transaction))
    lazy val inputContracts = transaction.transaction.inputContracts
    lazy val updatedContractKeys = transaction.transaction.updatedContractKeys
    lazy val consumedContracts = transaction.transaction.consumedContracts

    Timed.future(
      metrics.daml.SoX.precomputeTransactionOutputs,
      Future(contractKeyInputs).map(
        _.map((_, inputContracts, updatedContractKeys, consumedContracts))
      ),
    )
  }

  private def conflictCheckWithDelta
      : () => Either[SoxRejection, (Offset, Transaction, TransactionEffects)] => Iterable[
        (Offset, Update)
      ] = () => {
    @volatile var sequencerQueueState: SequencerState = SequencerState()(metrics)

    {
      case Left(rejection) =>
        Timed.value(
          metrics.daml.SoX.sequenceDuration, {
            val newIndex = offsetIdx.incrementAndGet()
            val newOffset = toOffset(newIndex)
            Iterable(newOffset -> toRejection(rejection))
          },
        )
      case Right(
            (
              noConflictUpTo,
              transaction,
              (keyInputs, inputContracts, updatedKeys, consumedContracts),
            )
          ) =>
        Timed.value(
          metrics.daml.SoX.sequenceDuration, {
            val newIndex = offsetIdx.incrementAndGet()
            val newOffset = toOffset(newIndex)
            conflictCheckWithInTransit(
              sequencerQueueState,
              keyInputs,
              inputContracts,
              transaction,
            ) match {
              case Left(rejection) => Iterable(newOffset -> toRejection(rejection))
              case Right(acceptedTx) =>
                sequencerQueueState = sequencerQueueState
                  .dequeue(noConflictUpTo)
                  .enqueue(newOffset, updatedKeys, consumedContracts)

                Iterable(newOffset -> successMapper(acceptedTx, newIndex, participantId))
            }
          },
        )
    }
  }

  private def invalidInputFromParticipant(
      originalTx: Submission.Transaction
  ): com.daml.lf.transaction.Transaction.KeyInputError => SoxRejection = {
    case com.daml.lf.transaction.Transaction.InconsistentKeys(key) =>
      GenericRejectionFailure(s"Inconsistent keys for $key")(originalTx)
    case com.daml.lf.transaction.Transaction.DuplicateKeys(key) =>
      GenericRejectionFailure(s"Duplicate key for $key")(originalTx)
  }

  private def conflictCheckWithInTransit(
      sequencerQueueState: SequencerState,
      keyInputs: KeyInputs,
      inputContracts: Set[ContractId],
      transaction: Submission.Transaction,
  ): Either[SoxRejection, Submission.Transaction] = {
    val updatedKeys = sequencerQueueState.keyState
    val archives = sequencerQueueState.consumedContractsState

    keyInputs
      .foldLeft[Either[SoxRejection, Unit]](Right(())) {
        case (Right(_), (key, KeyCreate)) =>
          updatedKeys.get(key) match {
            case Some((None, _)) | None => Right(())
            case Some((Some(_), _)) => Left(DuplicateKey(key)(transaction))
          }
        case (Right(_), (key, NegativeKeyLookup)) =>
          updatedKeys.get(key) match {
            case Some((None, _)) | None => Right(())
            case Some((Some(actual), _)) =>
              Left(InconsistentContractKey(None, Some(actual))(transaction))
          }
        case (Right(_), (key, KeyActive(cid))) =>
          updatedKeys.get(key) match {
            case Some((Some(`cid`), _)) | None => Right(())
            case Some((other, _)) => Left(InconsistentContractKey(other, Some(cid))(transaction))
          }
        case (left, _) => left
      }
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

  private def conflictCheckWithCommitted(
      transaction: Submission.Transaction,
      inputContracts: Set[ContractId],
      keyInputs: KeyInputs,
  ): Future[Either[SoxRejection, Unit]] =
    Timed.future(
      metrics.daml.SoX.conflictCheckWithCommitted,
      checkTimeModel(transaction) match {
        case Left(value) => Future(Left(value))
        case Right(_) =>
          validateCausalMonotonicity(
            transaction,
            inputContracts,
            transaction.transactionMeta.ledgerEffectiveTime,
            transaction.blindingInfo.divulgence.keySet,
          ).flatMap {
            case Right(_) => validatePartyAllocation(transaction)
            case rejection => Future.successful(rejection)
          }.flatMap {
            case Right(_) => validateKeyUsages(transaction, keyInputs)
            case rejection => Future.successful(rejection)
          }
      },
    )

  private def validatePartyAllocation(
      transaction: Submission.Transaction
  ): Future[Either[SoxRejection, Unit]] = {
    val informees = transaction.transaction.informees
    indexService
      .getParties(informees.toSeq)
      .map { partyDetails =>
        val allocatedInformees = partyDetails.iterator.map(_.party).toSet
        if (allocatedInformees == informees) Right(())
        else
          Left(
            SoxRejection.UnallocatedParties((informees diff allocatedInformees).toSet)(transaction)
          )
      }
  }

  private def checkTimeModel(
      transaction: Transaction
  ): Either[SoxRejection, Unit] = {
    // TODO SoX: Support other time providers
    val recordTime = Timestamp.now()

    currentConfiguration
      .get()
      .toRight(SoxRejection.NoLedgerConfiguration(transaction))
      .flatMap(
        _.timeModel
          .checkTime(transaction.transactionMeta.ledgerEffectiveTime, recordTime)
          .left
          .map(SoxRejection.InvalidLedgerTime(_)(transaction))
      )
  }

  private def validateCausalMonotonicity(
      transaction: Transaction,
      inputContracts: Set[ContractId],
      transactionLedgerEffectiveTime: Timestamp,
      divulged: Set[ContractId],
  ): Future[Either[SoxRejection, Unit]] = {
    val referredContracts = inputContracts.diff(divulged)
    if (referredContracts.isEmpty)
      Future.successful(Right(()))
    else
      indexService
        .lookupMaximumLedgerTime(referredContracts)
        .transform {
          case Failure(MissingContracts(missingContractIds)) =>
            Success(Left(UnknownContracts(missingContractIds)(transaction)))
          case Failure(err) =>
            Success(Left(GenericRejectionFailure(err.getMessage)(transaction)))
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

  private def validateKeyUsages(
      transaction: Transaction,
      keyInputs: KeyInputs,
  ): Future[Either[SoxRejection, Unit]] =
    keyInputs.foldLeft(Future.successful[Either[SoxRejection, Unit]](Right(()))) {
      case (f, (key, inputState)) =>
        f.flatMap {
          case Right(_) => validateExpectedKey(transaction, key, inputState)
          case left => Future.successful(left)
        }
    }

  private def validateExpectedKey(
      transaction: Submission.Transaction,
      key: Key,
      inputState: TxKeyInput,
  ): Future[Either[SoxRejection, Unit]] =
    indexService
      // TODO SoX: Is it fine to use informees as readers?
      .lookupContractKey(transaction.transaction.informees, key)
      .map { lookupResult =>
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

  Sink
    .fromSubscriber(readServiceWithFeedSubscriber.subscriber)
    .runWith(source merge queueSource)

  override def close(): Unit = {
    logger.info("Shutting down ConflictCheckingLedgerBridge.")
    queue.complete()
    conflictCheckingQueue.complete()
  }
}

object ConflictCheckingWriteService {
  private implicit val errorLoggingContext: ContextualizedErrorLogger = NoLogging

  def owner(
      readServiceWithFeedSubscriber: ReadServiceWithFeedSubscriber,
      participantId: Ref.ParticipantId,
      ledgerId: LedgerId,
      maxDedupSeconds: Int,
      submissionBufferSize: Int,
      indexService: IndexService,
      metrics: Metrics,
      // TODO SoX: Wire up in config
      implicitPartyAllocation: Boolean = false,
  )(implicit
      mat: Materializer,
      loggingContext: LoggingContext,
      servicesExecutionContext: ExecutionContext,
  ): ResourceOwner[ConflictCheckingWriteService] = ResourceOwner.forCloseable(() =>
    new ConflictCheckingWriteService(
      readServiceWithFeedSubscriber = readServiceWithFeedSubscriber,
      participantId = participantId,
      ledgerId = ledgerId,
      maxDedupSeconds = maxDedupSeconds,
      submissionBufferSize = submissionBufferSize,
      indexService = indexService,
      metrics = metrics,
      implicitPartyAllocation = implicitPartyAllocation,
    )
  )

  trait Submission extends Serializable with Product
  object Submission {
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
  def fromOffset(offset: Offset): Long = Longs.fromByteArray(paddedTo8(offset.toByteArray))

  private def paddedTo8(in: Array[Byte]): Array[Byte] = if (in.length > 8)
    throw new RuntimeException(s"byte array too big: ${in.length}")
  else Array.fill[Byte](8 - in.length)(0) ++ in

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
