// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.BridgeMetrics
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.ledger.participant.state.v2.{CompletionInfo, Update}
import com.daml.ledger.sandbox.ConflictCheckingLedgerBridge._
import com.daml.ledger.sandbox.LedgerBridge.{fromOffset, successMapper, toOffset}
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.Submission.{AsyncValidationStep, Transaction, Validated}
import com.daml.ledger.sandbox.domain.{
  NoOpPreparedSubmission,
  PreparedSubmission,
  PreparedTransactionSubmission,
  Rejection,
  Submission,
}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.CommittedTransaction
import com.daml.lf.transaction.Transaction.{
  KeyActive,
  KeyCreate,
  NegativeKeyLookup,
  KeyInput => TxKeyInput,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Timed
import com.daml.platform.ApiOffset
import com.daml.platform.apiserver.execution.MissingContracts
import com.daml.platform.store.appendonlydao.events._
import com.google.common.primitives.Longs

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait LedgerBridge {
  def flow: Flow[Submission, (Offset, Update), NotUsed]
}

class ConflictCheckingLedgerBridge(
    participantId: Ref.ParticipantId,
    indexService: IndexService,
    initialLedgerEnd: Offset,
    bridgeMetrics: BridgeMetrics,
)(implicit
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) extends LedgerBridge {
  private[this] implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private[this] lazy val offsetIdx = new AtomicLong(fromOffset(initialLedgerEnd))

  def flow: Flow[Submission, (Offset, Update), NotUsed] =
    Flow[Submission]
      .mapAsyncUnordered(64)(prepareSubmission)
      .mapAsync(1)(tagWithLedgerEnd)
      .mapAsync(64)(conflictCheckWithCommitted)
      .statefulMapConcat(conflictCheckWithDelta)

  private def prepareSubmission(submission: Submission): AsyncValidationStep[PreparedSubmission] =
    submission match {
      case tx @ Transaction(submitterInfo, _, transaction, _) =>
        withErrorLogger(submitterInfo.submissionId) { implicit errorLogger =>
          Timed.future(
            bridgeMetrics.Stages.precomputeTransactionOutputs,
            Future(
              transaction.transaction.contractKeyInputs.left
                .map(invalidInputFromParticipant(submitterInfo.toCompletionInfo))
            ).map(
              _.map(
                PreparedTransactionSubmission(
                  _,
                  transaction.transaction.inputContracts,
                  transaction.transaction.updatedContractKeys,
                  transaction.transaction.consumedContracts,
                  tx,
                )
              )
            ),
          )
        }
      case other => Future.successful(Right(NoOpPreparedSubmission(other)))
    }

  private def tagWithLedgerEnd(
      validated: Validated[PreparedSubmission]
  ): AsyncValidationStep[(Offset, PreparedSubmission)] =
    validated match {
      case Left(rejection) =>
        Future.successful(Left(rejection))
      case Right(preparedSubmission) =>
        indexService
          .currentLedgerEnd()
          .map(ledgerEnd =>
            Right(ApiOffset.assertFromString(ledgerEnd.value) -> preparedSubmission)
          )
    }

  private def conflictCheckWithCommitted(
      in: Validated[(Offset, PreparedSubmission)]
  ): AsyncValidationStep[(Offset, PreparedSubmission)] =
    in match {
      case Left(rejection) => Future.successful(Left(rejection))
      case Right(validated @ (_, preparedSubmission)) =>
        preparedSubmission match {
          case PreparedTransactionSubmission(keyInputs, inputContracts, _, _, tx) =>
            withErrorLogger(tx.submitterInfo.submissionId) { implicit errorLogger =>
              conflictCheckWithCommitted(tx, inputContracts, keyInputs).map(_.map(_ => validated))
            }
          case _ => Future.successful(Right(validated))
        }
    }
  private def conflictCheckWithDelta
      : () => Validated[(Offset, PreparedSubmission)] => Iterable[(Offset, Update)] = () => {
    @volatile var sequencerQueueState: SequencerState = SequencerState()(bridgeMetrics)

    in => {
      Timed.value(
        bridgeMetrics.Stages.conflictCheckWithDelta,
        in match {
          case Left(rejection) =>
            val newIndex = offsetIdx.incrementAndGet()
            val newOffset = toOffset(newIndex)
            Iterable(newOffset -> rejection)
          case Right((noConflictUpTo, txSubmission: PreparedTransactionSubmission)) =>
            withErrorLogger(txSubmission.originalTx.submitterInfo.submissionId) {
              implicit errorLogger =>
                val newIndex = offsetIdx.incrementAndGet()
                val newOffset = toOffset(newIndex)
                conflictCheckWithInTransit(
                  sequencerQueueState,
                  txSubmission.keyInputs,
                  txSubmission.inputContracts,
                  txSubmission.originalTx,
                ) match {
                  case Left(rejection) =>
                    Iterable(
                      newOffset -> toRejection(
                        rejection,
                        txSubmission.originalTx.submitterInfo.toCompletionInfo,
                      )
                    )
                  case Right(acceptedTx) =>
                    sequencerQueueState = sequencerQueueState
                      .dequeue(noConflictUpTo)
                      .enqueue(newOffset, txSubmission.updatedKeys, txSubmission.consumedContracts)

                    Iterable(newOffset -> successMapper(acceptedTx, newIndex, participantId))
                }
            }

          case Right((_, NoOpPreparedSubmission(other))) =>
            val newIndex = offsetIdx.incrementAndGet()
            val newOffset = toOffset(newIndex)
            Iterable(newOffset -> successMapper(other, newIndex, participantId))
        },
      )
    }
  }

  private def withErrorLogger[T](submissionId: Option[String])(
      f: ContextualizedErrorLogger => T
  )(implicit loggingContext: LoggingContext, logger: ContextualizedLogger) = {
    val contextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, submissionId)
    f(contextualizedErrorLogger)
  }

  private def conflictCheckWithCommitted(
      transaction: Submission.Transaction,
      inputContracts: Set[ContractId],
      keyInputs: KeyInputs,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Either[Update.CommandRejected, Unit]] =
    Timed.future(
      bridgeMetrics.Stages.conflictCheckWithCommitted,
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

  private def invalidInputFromParticipant(
      completionInfo: CompletionInfo
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): com.daml.lf.transaction.Transaction.KeyInputError => Update.CommandRejected = {
    case com.daml.lf.transaction.Transaction.InconsistentKeys(key) =>
      toRejection(GenericRejectionFailure(s"Inconsistent keys for $key"), completionInfo)
    case com.daml.lf.transaction.Transaction.DuplicateKeys(key) =>
      toRejection(GenericRejectionFailure(s"Duplicate key for $key"), completionInfo)
  }

  private def conflictCheckWithInTransit(
      sequencerQueueState: SequencerState,
      keyInputs: KeyInputs,
      inputContracts: Set[ContractId],
      transaction: Submission.Transaction,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[Rejection, Submission.Transaction] = {
    val updatedKeys = sequencerQueueState.keyState
    val archives = sequencerQueueState.consumedContractsState

    keyInputs
      .foldLeft[Either[Rejection, Unit]](Right(())) {
        case (Right(_), (key, KeyCreate)) =>
          updatedKeys.get(key) match {
            case Some((None, _)) | None => Right(())
            case Some((Some(_), _)) => Left(DuplicateKey(key))
          }
        case (Right(_), (key, NegativeKeyLookup)) =>
          updatedKeys.get(key) match {
            case Some((None, _)) | None => Right(())
            case Some((Some(actual), _)) =>
              Left(InconsistentContractKey(None, Some(actual)))
          }
        case (Right(_), (key, KeyActive(cid))) =>
          updatedKeys.get(key) match {
            case Some((Some(`cid`), _)) | None => Right(())
            case Some((other, _)) => Left(InconsistentContractKey(other, Some(cid)))
          }
        case (left, _) => left
      }
      .flatMap { _ =>
        val alreadyArchived = inputContracts.intersect(archives)
        if (alreadyArchived.nonEmpty) Left(UnknownContracts(alreadyArchived))
        else Right(())
      }
      .map(_ => transaction)
  }

  private def toRejection(rejection: Rejection, completionInfo: CompletionInfo) =
    Update.CommandRejected(
      recordTime = Timestamp.now(),
      completionInfo = completionInfo,
      reasonTemplate = FinalReason(rejection.toStatus),
    )

  private def validatePartyAllocation(
      transaction: Submission.Transaction
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Either[Update.CommandRejected, Unit]] = {
    val informees = transaction.transaction.informees
    indexService
      .getParties(informees.toSeq)
      .map { partyDetails =>
        val allocatedInformees = partyDetails.iterator.map(_.party).toSet
        if (allocatedInformees == informees) Right(())
        else
          Left(
            toRejection(
              Rejection.UnallocatedParties((informees diff allocatedInformees).toSet),
              transaction.submitterInfo.toCompletionInfo,
            )
          )
      }
  }

  private def checkTimeModel(
      transaction: Transaction
  ): Either[Update.CommandRejected, Unit] = {
    val _ = transaction
    Right(())
  }

  private def validateCausalMonotonicity(
      transaction: Transaction,
      inputContracts: Set[ContractId],
      transactionLedgerEffectiveTime: Timestamp,
      divulged: Set[ContractId],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Either[Update.CommandRejected, Unit]] = {
    val referredContracts = inputContracts.diff(divulged)
    if (referredContracts.isEmpty)
      Future.successful(Right(()))
    else
      indexService
        .lookupMaximumLedgerTime(referredContracts)
        .transform {
          case Failure(MissingContracts(missingContractIds)) =>
            Success(
              Left(
                toRejection(
                  UnknownContracts(missingContractIds),
                  transaction.submitterInfo.toCompletionInfo,
                )
              )
            )
          case Failure(err) =>
            Success(
              Left(
                toRejection(
                  GenericRejectionFailure(err.getMessage),
                  transaction.submitterInfo.toCompletionInfo,
                )
              )
            )
          case Success(maximumLedgerEffectiveTime) =>
            maximumLedgerEffectiveTime
              .filter(_ > transactionLedgerEffectiveTime)
              .fold[Try[Either[Update.CommandRejected, Unit]]](Success(Right(())))(
                contractLedgerEffectiveTime =>
                  Success(
                    Left(
                      toRejection(
                        CausalMonotonicityViolation(
                          contractLedgerEffectiveTime = contractLedgerEffectiveTime,
                          transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
                        ),
                        transaction.submitterInfo.toCompletionInfo,
                      )
                    )
                  )
              )
        }
  }

  private def validateKeyUsages(
      transaction: Transaction,
      keyInputs: KeyInputs,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) =
    keyInputs.foldLeft(Future.successful[Either[Update.CommandRejected, Unit]](Right(()))) {
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
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Either[Update.CommandRejected, Unit]] =
    indexService
      // TODO SoX: Is it fine to use informees as readers?
      .lookupContractKey(transaction.transaction.informees, key)
      .map { lookupResult =>
        (inputState, lookupResult) match {
          case (NegativeKeyLookup, Some(actual)) =>
            Left(InconsistentContractKey(None, Some(actual)))
          case (KeyCreate, Some(_)) =>
            Left(DuplicateKey(key))
          case (KeyActive(expected), actual) if !actual.contains(expected) =>
            Left(InconsistentContractKey(Some(expected), actual))
          case _ => Right(())
        }
      }
      .map(_.left.map(toRejection(_, transaction.submitterInfo.toCompletionInfo)))
}

object ConflictCheckingLedgerBridge {
  private type KeyInputs = Map[Key, TxKeyInput]
}

class PassThroughLedgerBridge(participantId: Ref.ParticipantId) extends LedgerBridge {
  override def flow: Flow[Submission, (Offset, Update), NotUsed] =
    Flow[Submission].zipWithIndex
      .map { case (submission, index) =>
        (toOffset(index), successMapper(submission, index, participantId))
      }
}

object LedgerBridge {
  def toOffset(index: Long): Offset = Offset.fromByteArray(Longs.toByteArray(index))
  def fromOffset(offset: Offset): Long = Longs.fromByteArray(paddedTo8(offset.toByteArray))

  def successMapper(
      submission: Submission,
      index: Long,
      participantId: Ref.ParticipantId,
  ): Update =
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

  private def paddedTo8(in: Array[Byte]): Array[Byte] = if (in.length > 8)
    throw new RuntimeException(s"byte array too big: ${in.length}")
  else Array.fill[Byte](8 - in.length)(0) ++ in
}
