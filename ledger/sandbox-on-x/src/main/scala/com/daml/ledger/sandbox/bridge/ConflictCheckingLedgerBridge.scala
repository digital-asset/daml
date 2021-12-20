// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox.bridge

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.ledger.participant.state.v2.{CompletionInfo, Update}
import com.daml.ledger.sandbox.bridge.ConflictCheckingLedgerBridge._
import com.daml.ledger.sandbox.bridge.LedgerBridge.{fromOffset, successMapper, toOffset}
import com.daml.ledger.sandbox.bridge.SequencerState.LastUpdatedAt
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.Submission.Transaction
import com.daml.ledger.sandbox.domain._
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.{Transaction => LfTransaction}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Timed
import com.daml.platform.ApiOffset
import com.daml.platform.apiserver.execution.MissingContracts
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.events._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._
import scala.util.{Failure, Success, Try}

private[sandbox] class ConflictCheckingLedgerBridge(
    participantId: Ref.ParticipantId,
    indexService: IndexService,
    initialLedgerEnd: Offset,
    bridgeMetrics: BridgeMetrics,
    errorFactories: ErrorFactories,
)(implicit
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) extends LedgerBridge {
  private val AsyncStagesParallelism = 64
  private[this] implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  def flow: Flow[Submission, (Offset, Update), NotUsed] =
    Flow[Submission]
      .mapAsyncUnordered(AsyncStagesParallelism)(prepareSubmission)
      .mapAsync(parallelism = 1)(tagWithLedgerEnd)
      .mapAsync(AsyncStagesParallelism)(conflictCheckWithCommitted)
      .statefulMapConcat(sequence)

  // This stage precomputes the transaction effects for transaction submissions.
  // For other update types, this stage is a no-op.
  private val prepareSubmission: PrepareSubmission = {
    case transactionSubmission @ Submission.Transaction(submitterInfo, _, transaction, _) =>
      Timed.future(
        bridgeMetrics.Stages.precomputeTransactionOutputs,
        Future(
          transaction.transaction.contractKeyInputs
            .map(contractKeyInputs =>
              PreparedTransactionSubmission(
                contractKeyInputs,
                transaction.transaction.inputContracts,
                transaction.transaction.updatedContractKeys,
                transaction.transaction.consumedContracts,
                Blinding.blind(transaction),
                transactionSubmission,
              )
            )
            .left
            .map(
              withErrorLogger(submitterInfo.submissionId)(
                invalidInputFromParticipantRejection(submitterInfo.toCompletionInfo())(_)
              )
            )
        ),
      )
    case other => Future.successful(Right(NoOpPreparedSubmission(other)))
  }

  // Tags the prepared submission with the current ledger end as available on the Ledger API.
  private val tagWithLedgerEnd: TagWithLedgerEnd = {
    case Left(rejection) =>
      Future.successful(Left(rejection))
    case Right(preparedSubmission) =>
      indexService
        .currentLedgerEnd()
        .map(ledgerEnd => Right(ApiOffset.assertFromString(ledgerEnd.value) -> preparedSubmission))
  }

  // This stage performs conflict checking for incoming submissions against the ledger state
  // as it is visible on the Ledger API.
  private val conflictCheckWithCommitted: ConflictCheckWithCommitted = {
    case Left(rejection) => Future.successful(Left(rejection))
    case Right(
          validated @ (
            _,
            PreparedTransactionSubmission(
              keyInputs,
              inputContracts,
              _,
              _,
              blindingInfo,
              originalSubmission,
            ),
          )
        ) =>
      withErrorLogger(originalSubmission.submitterInfo.submissionId) { implicit errorLogger =>
        Timed
          .future(
            bridgeMetrics.Stages.conflictCheckWithCommitted,
            checkTimeModel(originalSubmission) match {
              case Left(value) => Future(Left(value))
              case Right(_) =>
                validateCausalMonotonicity(
                  transaction = originalSubmission,
                  inputContracts = inputContracts,
                  transactionLedgerEffectiveTime =
                    originalSubmission.transactionMeta.ledgerEffectiveTime,
                  divulged = blindingInfo.divulgence.keySet,
                ).flatMap {
                  case Right(_) => validatePartyAllocation(originalSubmission)
                  case rejection => Future.successful(rejection)
                }.flatMap {
                  case Right(_) => validateKeyUsages(originalSubmission, keyInputs)
                  case rejection => Future.successful(rejection)
                }
            },
          )
          .map(_.map(_ => validated))
      }
    case Right(validated) => Future.successful(Right(validated))
  }

  // This stage performs sequential conflict checking with the in-flight commands,
  // assigns offsets and converts the accepted/rejected commands to updates.
  private val sequence: Sequence = () => {
    @volatile var sequencerQueueState: SequencerState = SequencerState()(bridgeMetrics)
    @volatile var offsetIdx: Long = fromOffset(initialLedgerEnd)

    in => {
      Timed.value(
        bridgeMetrics.Stages.sequence, {
          offsetIdx = offsetIdx + 1
          val newOffset = toOffset(offsetIdx)

          val update = in match {
            case Left(rejection) => rejection
            case Right((_, NoOpPreparedSubmission(other))) =>
              successMapper(other, offsetIdx, participantId)
            case Right((noConflictUpTo, txSubmission: PreparedTransactionSubmission)) =>
              val submitterInfo = txSubmission.originalSubmission.submitterInfo

              withErrorLogger(submitterInfo.submissionId) { implicit errorLogger =>
                conflictCheckWithInFlight(
                  keysState = sequencerQueueState.keyState,
                  consumedContractsState = sequencerQueueState.consumedContractsState,
                  keyInputs = txSubmission.keyInputs,
                  inputContracts = txSubmission.inputContracts,
                )
              }.fold(
                toCommandRejectedUpdate(_, submitterInfo.toCompletionInfo()),
                { _ =>
                  // Update the sequencer state
                  sequencerQueueState = sequencerQueueState
                    .dequeue(noConflictUpTo)
                    .enqueue(newOffset, txSubmission.updatedKeys, txSubmission.consumedContracts)

                  successMapper(txSubmission.originalSubmission, offsetIdx, participantId)
                },
              )
          }

          Iterable(newOffset -> update)
        },
      )
    }
  }

  private def conflictCheckWithInFlight(
      keysState: Map[Key, (Option[ContractId], LastUpdatedAt)],
      consumedContractsState: Set[ContractId],
      keyInputs: KeyInputs,
      inputContracts: Set[ContractId],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[Rejection, Unit] =
    inputContracts.intersect(consumedContractsState) pipe {
      case alreadyArchived if alreadyArchived.nonEmpty =>
        Left(UnknownContracts(alreadyArchived)(errorFactories))
      case _ =>
        keyInputs
          .foldLeft[Either[Rejection, Unit]](Right(())) {
            case (Right(_), (key, LfTransaction.KeyCreate)) =>
              keysState.get(key) match {
                case None | Some((None, _)) => Right(())
                case Some((Some(_), _)) => Left(DuplicateKey(key)(errorFactories))
              }
            case (Right(_), (key, LfTransaction.NegativeKeyLookup)) =>
              keysState.get(key) match {
                case None | Some((None, _)) => Right(())
                case Some((Some(actual), _)) =>
                  Left(InconsistentContractKey(None, Some(actual))(errorFactories))
              }
            case (Right(_), (key, LfTransaction.KeyActive(cid))) =>
              keysState.get(key) match {
                case None | Some((Some(`cid`), _)) => Right(())
                case Some((other, _)) =>
                  Left(InconsistentContractKey(other, Some(cid))(errorFactories))
              }
            case (left, _) => left
          }
    }

  private def validatePartyAllocation(
      transaction: Submission.Transaction
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): AsyncValidation[Unit] = {
    val _ = (transaction, contextualizedErrorLogger)
    // TODO SoX: Implement
    Future.successful(Right(()))
  }

  private def checkTimeModel(
      transaction: Transaction
  ): Validated[Unit] = {
    // TODO SoX: Implement
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
  ): AsyncValidation[Unit] = {
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
                toCommandRejectedUpdate(
                  UnknownContracts(missingContractIds)(errorFactories),
                  transaction.submitterInfo.toCompletionInfo(),
                )
              )
            )
          case Failure(err) =>
            Success(
              Left(
                toCommandRejectedUpdate(
                  LedgerBridgeInternalError(err),
                  transaction.submitterInfo.toCompletionInfo(),
                )
              )
            )
          case Success(maximumLedgerEffectiveTime) =>
            maximumLedgerEffectiveTime
              .filter(_ > transactionLedgerEffectiveTime)
              .fold[Try[Validated[Unit]]](Success(Right(())))(contractLedgerEffectiveTime =>
                Success(
                  Left(
                    toCommandRejectedUpdate(
                      CausalMonotonicityViolation(
                        contractLedgerEffectiveTime = contractLedgerEffectiveTime,
                        transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
                      )(errorFactories),
                      transaction.submitterInfo.toCompletionInfo(),
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
  ): AsyncValidation[Unit] =
    keyInputs.foldLeft(Future.successful[Validated[Unit]](Right(()))) {
      case (f, (key, inputState)) =>
        f.flatMap {
          case Right(_) =>
            indexService
              // TODO SoX: Perform lookup more efficiently and do not use a readers-based lookup
              .lookupContractKey(transaction.transaction.informees, key)
              .map { lookupResult =>
                (inputState, lookupResult) match {
                  case (LfTransaction.NegativeKeyLookup, Some(actual)) =>
                    Left(InconsistentContractKey(None, Some(actual))(errorFactories))
                  case (LfTransaction.KeyCreate, Some(_)) =>
                    Left(DuplicateKey(key)(errorFactories))
                  case (LfTransaction.KeyActive(expected), actual) if !actual.contains(expected) =>
                    Left(InconsistentContractKey(Some(expected), actual)(errorFactories))
                  case _ => Right(())
                }
              }
              .map(
                _.left.map(toCommandRejectedUpdate(_, transaction.submitterInfo.toCompletionInfo()))
              )
          case left => Future.successful(left)
        }
    }
}

object ConflictCheckingLedgerBridge {
  private type Validated[T] = Either[Update.CommandRejected, T]
  private type AsyncValidation[T] = Future[Validated[T]]
  private type KeyInputs = Map[Key, LfTransaction.KeyInput]

  // Conflict checking stages
  private type PrepareSubmission = Submission => AsyncValidation[PreparedSubmission]
  private type TagWithLedgerEnd =
    Validated[PreparedSubmission] => AsyncValidation[(Offset, PreparedSubmission)]
  private type ConflictCheckWithCommitted =
    Validated[(Offset, PreparedSubmission)] => AsyncValidation[(Offset, PreparedSubmission)]
  private type Sequence =
    () => Validated[(Offset, PreparedSubmission)] => Iterable[(Offset, Update)]

  private def toCommandRejectedUpdate(rejection: Rejection, completionInfo: CompletionInfo) =
    Update.CommandRejected(
      recordTime = Timestamp.now(),
      completionInfo = completionInfo,
      reasonTemplate = FinalReason(rejection.toStatus),
    )

  private def invalidInputFromParticipantRejection(
      completionInfo: CompletionInfo
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): LfTransaction.KeyInputError => Update.CommandRejected = {
    case LfTransaction.InconsistentKeys(key) =>
      toCommandRejectedUpdate(
        TransactionInternallyInconsistentKey(key),
        completionInfo,
      )
    case LfTransaction.DuplicateKeys(key) =>
      toCommandRejectedUpdate(TransactionInternallyInconsistentContract(key), completionInfo)
  }

  private def withErrorLogger[T](submissionId: Option[String])(
      f: ContextualizedErrorLogger => T
  )(implicit loggingContext: LoggingContext, logger: ContextualizedLogger) =
    f(new DamlContextualizedErrorLogger(logger, loggingContext, submissionId))
}
