// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import com.daml.daml_lf_dev.DamlLf.Archive
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
import com.daml.lf.transaction.Node.Rollback
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction, GlobalKey, SubmittedTransaction}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
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
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

case class ReadWriteServiceBridge(
    participantId: Ref.ParticipantId,
    ledgerId: LedgerId,
    maxDedupSeconds: Int,
    submissionBufferSize: Int,
    contractStore: AtomicReference[Option[ContractStore]],
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends ReadService
    with WriteService
    with AutoCloseable {
  private val offsetIdx = new AtomicLong(0L)

  import ReadWriteServiceBridge._
  private implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

  private[this] val logger = ContextualizedLogger.get(getClass)

  private val (conflictCheckingQueue, source) = Source
    .queue[Submission.Transaction](128)
    .mapAsyncUnordered(64) { tx =>
      val enqueuedAt = contractStore
        .get()
        .getOrElse(throw new RuntimeException("ContractStore not there yet"))
        .cacheOffset()
      parallelConflictCheck(tx).map(enqueuedAt -> _)
    }
    .statefulMapConcat(sequence)
    .preMaterialize()

  private def conflictCheckSlice(
      delta: Vector[(Offset, Submission.Transaction)],
      transaction: Submission.Transaction,
  ): Either[SoxRejection, Submission.Transaction] = {
    val (cKeys, _, archives) = collectDelta(delta)
    transaction.transaction
      .fold[Either[SoxRejection, Unit]](Right(())) {
        case (Right(_), (_, c: Create)) =>
          c.key
            .map(convert(c.templateId, _))
            .map { key =>
              if (cKeys.get(key).flatten.nonEmpty)
                Left(DuplicateKey(key)(transaction))
              else Right(())
            }
            .getOrElse(Right(()))
        case (Right(_), (_, exercise: Exercise)) =>
          if (archives(exercise.targetCoid))
            Left(UnknownContracts(Set(exercise.targetCoid))(transaction))
          else Right(())
        case (Right(_), (_, lookupByKey: LookupByKey)) =>
          cKeys.get(convert(lookupByKey.templateId, lookupByKey.key)) match {
            case Some(result) =>
              if (result == lookupByKey.result) Right(())
              else Left(InconsistentContracts(lookupByKey.result, result)(transaction))
            case None => Right(())
          }
        case (Right(_), (_, fetch: Fetch)) =>
          if (archives(fetch.coid)) Left(UnknownContracts(Set(fetch.coid))(transaction))
          else Right(())
        case (Right(_), (_, Rollback(_))) => Right(())
        case (left @ Left(_), _) => left
      }
      .map(_ => transaction)
  }

  private def sequence: () => ((Offset, Either[SoxRejection, Transaction])) => Iterable[
    (Offset, Update)
  ] = () => {
    var sequencerQueue = Vector.empty[(Offset, Submission.Transaction)]

    {
      case (_, Left(rejection)) =>
        val newIndex = offsetIdx.getAndIncrement()
        val newOffset = toOffset(newIndex)
        Iterable(newOffset -> toRejection(rejection))
      case (enqueuedAt, Right(transaction)) =>
        val newIndex = offsetIdx.getAndIncrement()
        val newOffset = toOffset(newIndex)
        checkSequential(enqueuedAt, transaction, sequencerQueue) match {
          case Left(rejection) =>
            Iterable(
              newOffset -> toRejection(rejection)
            )
          case Right(acceptedTx) =>
            sequencerQueue = sequencerQueue :+ (newOffset -> transaction)
            Iterable(newOffset -> successMapper(acceptedTx, newIndex, participantId))
        }
    }
  }

  private def toRejection(rejection: SoxRejection) = {
    Update.CommandRejected(
      recordTime = Timestamp.now(),
      completionInfo = rejection.originalTx.submitterInfo.toCompletionInfo,
      reasonTemplate = FinalReason({
        Status.of(Code.INVALID_ARGUMENT.value, rejection.toString, Seq.empty)
      }),
    )
  }

  private def checkSequential(
      noConflictUpTo: Offset,
      transaction: Submission.Transaction,
      sequencerQueue: Vector[(Offset, Submission.Transaction)],
  ): Either[SoxRejection, Submission.Transaction] = {
    val idx = sequencerQueue.view.map(_._1).search(noConflictUpTo) match {
      case Searching.Found(foundIndex) => foundIndex + 1
      case Searching.InsertionPoint(insertionPoint) => insertionPoint
    }

    if (sequencerQueue.size > idx) {
      conflictCheckSlice(sequencerQueue.slice(idx, sequencerQueue.length), transaction)
    } else Right(transaction)
  }

  private def parallelConflictCheck(
      transaction: Submission.Transaction
  ): Future[Either[SoxRejection, Submission.Transaction]] =
    validateCausalMonotonicity(
      transaction,
      transaction.transactionMeta.ledgerEffectiveTime,
      transaction.blindingInfo.divulgence.keySet,
    ).flatMap {
      case Right(_) => validateKeyUsages(transaction)
      case rejection => Future.successful(rejection)
    }.flatMap {
      case Right(_) => validateParties(transaction.transaction)
      case rejection => Future.successful(rejection)
    }.map(_.map(_ => transaction))

  private def validateParties(
      transaction: SubmittedTransaction
  ): Future[Either[SoxRejection, Unit]] = {
    val _ = transaction
    // TODO implement
    Future.successful(Right(()))
  }

  private def collectDelta(
      delta: Vector[(Offset, Submission.Transaction)]
  ): (Map[Key, Option[ContractId]], Set[ContractId], Set[ContractId]) = {
    delta.view
      .map(_._2)
      .foldLeft(
        (Map.empty[GlobalKey, Option[ContractId]], Set.empty[ContractId], Set.empty[ContractId])
      ) { case (map, tx) =>
        tx.transaction.fold(map) {
          case (s @ (cKeys, creates, archives), (_, c: Create)) =>
            c.key.fold(s) { k =>
              (
                cKeys.updated(convert(c.templateId, k), Some(c.coid)),
                creates.incl(c.coid),
                archives,
              )
            }

          case (s @ (cKeys, creates, archives), (_, consume: Exercise)) if consume.consuming =>
            consume.key.fold(s)(key =>
              (
                cKeys.updated(convert(consume.templateId, key), None),
                creates,
                archives.incl(consume.targetCoid),
              )
            )
          case (s, _) => s
        }
      }
  }

  private def validateCausalMonotonicity(
      transaction: Transaction,
      transactionLedgerEffectiveTime: Timestamp,
      divulged: Set[ContractId],
  ): Future[Either[SoxRejection, Unit]] = {
    val referredContracts = transaction.transaction.inputContracts.diff(divulged)
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
                contractLedgerEffectiveTime => {
                  Success(
                    Left(
                      CausalMonotonicityViolation(
                        contractLedgerEffectiveTime = contractLedgerEffectiveTime,
                        transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
                      )(transaction)
                    )
                  )
                }
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
      transaction: Transaction
  ): Future[Either[SoxRejection, Unit]] = {
    val readers = transaction.transaction.informees // Is it informees??
    transaction.transaction
      .foldInExecutionOrder[Future[Either[SoxRejection, KeyValidationState]]](
        Future.successful(Right(KeyValidationState(Map.empty)))
      )(
        exerciseBegin = (acc, _, exe) => {
          acc.flatMap {
            case Right(keyValidationState) =>
              validateKeyUsages(transaction, readers, exe, keyValidationState)
            case left => Future.successful(left)
          } -> true
        },
        exerciseEnd = (acc, _, _) => acc,
        rollbackBegin = (acc, _, _) => acc -> false, // Do we care about rollback nodes?
        rollbackEnd = (acc, _, _) => acc,
        leaf = (acc, _, leaf) =>
          acc.flatMap {
            case Right(keyValidationState) =>
              validateKeyUsages(transaction, readers, leaf, keyValidationState)
            case left => Future.successful(left)
          },
      )
      .map(_.map(_ => ()))
  }

  case class KeyValidationState(keyStates: Map[Key, Option[ContractId]]) {
    def archive(key: Key): KeyValidationState =
      KeyValidationState(keyStates.updated(key, None))

    def create(key: Key, contractId: ContractId): KeyValidationState =
      KeyValidationState(keyStates.updated(key, Some(contractId)))
  }

  private def validateKeyUsages(
      transaction: Transaction,
      readers: Set[Ref.Party],
      node: Node,
      keyValidationState: KeyValidationState,
  ): Future[Either[SoxRejection, KeyValidationState]] =
    node match {
      case c: Create =>
        println(s"Create: $c")
        val maybeKey = c.key.map(convert(c.templateId, _))
        maybeKey.fold[Future[Either[SoxRejection, KeyValidationState]]](
          Future.successful(Right(keyValidationState))
        ) { key =>
          keyValidationState.keyStates.get(key) match {
            case Some(Some(_)) =>
              Future.successful(Left(DuplicateKey(key)(transaction)))
            case Some(None) =>
              Future.successful(Right(keyValidationState.create(key, c.coid)))
            case None =>
              contractStore
                .get()
                .getOrElse(throw new RuntimeException("ContractStore not there yet"))
                .lookupContractKey(readers, key)
                .map(
                  _.fold[Either[SoxRejection, KeyValidationState]](
                    Right(keyValidationState.create(key, c.coid))
                  )(_ => Left(DuplicateKey(key)(transaction)))
                )
          }
        }
      case l: LookupByKey =>
        val key = convert(l.templateId, l.key)
        keyValidationState.keyStates.get(key) match {
          case Some(result) =>
            Future.successful(
              Either.cond(
                result == l.result,
                keyValidationState,
                InconsistentContracts(l.result, result)(transaction),
              )
            )
          case None =>
            contractStore
              .get()
              .getOrElse(throw new RuntimeException("ContractStore not there yet"))
              .lookupContractKey(readers, key)
              .map { result =>
                Either.cond(
                  result == l.result,
                  keyValidationState,
                  InconsistentContracts(l.result, result)(transaction),
                )
              }
        }
      case exercise: Exercise if exercise.consuming =>
        println(s"Exercise: $exercise")
        Future.successful(Right(exercise.key.fold(keyValidationState) { k =>
          val key = convert(exercise.templateId, k)
          keyValidationState.archive(key)
        }))
      case other =>
        println(s"Other events: $other")
        // fetch and non-consuming exercise nodes don't need to validate
        // anything with regards to contract keys and do not alter the
        // state in a way which is relevant for the validation of
        // subsequent nodes
        //
        // `causalMonotonicity` already reports unknown contracts, no need to check it here
        Future.successful(Right(keyValidationState))
    }

  override def close(): Unit = {
    logger.info("Shutting down BridgeLedgerFactory.")
    queue.complete()
  }
}

object ReadWriteServiceBridge {
  trait Submission
  object Submission {
    sealed trait SoxRejection extends Submission {
      val originalTx: Submission.Transaction
    }
    final case class DuplicateKey(key: GlobalKey)(override val originalTx: Transaction)
        extends SoxRejection
    final case class InconsistentContracts(
        expectation: Option[ContractId],
        result: Option[ContractId],
    )(override val originalTx: Transaction)
        extends SoxRejection
    final case class GenericRejectionFailure(details: String)(override val originalTx: Transaction)
        extends SoxRejection
    final case class CausalMonotonicityViolation(
        contractLedgerEffectiveTime: Timestamp,
        transactionLedgerEffectiveTime: Timestamp,
    )(override val originalTx: Transaction)
        extends SoxRejection
    final case class UnknownContracts(ids: Set[ContractId])(override val originalTx: Transaction)
        extends SoxRejection

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
