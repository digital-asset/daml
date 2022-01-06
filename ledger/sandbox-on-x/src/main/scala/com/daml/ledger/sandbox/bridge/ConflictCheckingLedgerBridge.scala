// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.api.util.TimeProvider
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.ledger.participant.state.v2.{CompletionInfo, Update}
import com.daml.ledger.sandbox.bridge.ConflictCheckingLedgerBridge._
import com.daml.ledger.sandbox.bridge.LedgerBridge.{
  fromOffset,
  partyAllocationSuccessMapper,
  successMapper,
  toOffset,
}
import com.daml.ledger.sandbox.bridge.SequencerState.LastUpdatedAt
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.Submission.{AllocateParty, Config, Transaction}
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

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._
import scala.util.{Failure, Success, Try}

private[sandbox] class ConflictCheckingLedgerBridge(
    participantId: Ref.ParticipantId,
    indexService: IndexService,
    timeProvider: TimeProvider,
    initialLedgerEnd: Offset,
    initialAllocatedParties: Set[Ref.Party],
    initialLedgerConfiguration: Option[Configuration],
    bridgeMetrics: BridgeMetrics,
    errorFactories: ErrorFactories,
    validatePartyAllocation: Boolean,
    servicesThreadPoolSize: Int,
)(implicit
    servicesExecutionContext: ExecutionContext
) extends LedgerBridge {
  private[this] implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  @volatile private var allocatedParties = initialAllocatedParties
  @volatile private var ledgerConfiguration = initialLedgerConfiguration

  def flow: Flow[Submission, (Offset, Update), NotUsed] =
    Flow[Submission]
      // Set the parallelism as high as the bridgeThreadPoolSize
      .mapAsyncUnordered(servicesThreadPoolSize)(prepareSubmission)
      .mapAsync(parallelism = 1)(tagWithLedgerEnd)
      .mapAsync(servicesThreadPoolSize)(conflictCheckWithCommitted)
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
                transaction.informees,
                transactionSubmission,
              )
            )
            .left
            .map(
              withErrorLogger(submitterInfo.submissionId)(
                invalidInputFromParticipantRejection(submitterInfo.toCompletionInfo())(_)
              )(transactionSubmission.loggingContext, logger)
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
      Timed.future(
        bridgeMetrics.Stages.tagWithLedgerEnd,
        indexService
          .currentLedgerEnd()(preparedSubmission.submission.loggingContext)
          .map(ledgerEnd =>
            Right(ApiOffset.assertFromString(ledgerEnd.value) -> preparedSubmission)
          ),
      )
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
              transactionInformees,
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
                  case Right(_) => validateParties(originalSubmission, transactionInformees)
                  case rejection => Future.successful(rejection)
                }.flatMap {
                  case Right(_) => validateKeyUsages(originalSubmission, keyInputs)
                  case rejection => Future.successful(rejection)
                }
            },
          )
          .map(_.map(_ => validated))
      }(originalSubmission.loggingContext, logger)
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
            case Right((_, NoOpPreparedSubmission(submission))) =>
              processNonTransactionSubmission(offsetIdx, submission)
            case Right((noConflictUpTo, txSubmission: PreparedTransactionSubmission)) =>
              val submitterInfo = txSubmission.submission.submitterInfo

              withErrorLogger(submitterInfo.submissionId) { implicit errorLogger =>
                conflictCheckWithInFlight(
                  keysState = sequencerQueueState.keyState,
                  consumedContractsState = sequencerQueueState.consumedContractsState,
                  keyInputs = txSubmission.keyInputs,
                  inputContracts = txSubmission.inputContracts,
                )
              }(txSubmission.submission.loggingContext, logger).fold(
                toCommandRejectedUpdate(_, submitterInfo.toCompletionInfo()),
                { _ =>
                  // Update the sequencer state
                  sequencerQueueState = sequencerQueueState
                    .dequeue(noConflictUpTo)
                    .enqueue(newOffset, txSubmission.updatedKeys, txSubmission.consumedContracts)

                  successMapper(txSubmission.submission, offsetIdx, participantId)
                },
              )
          }

          Iterable(newOffset -> update)
        },
      )
    }
  }

  private def processNonTransactionSubmission(offsetIndex: Long, submission: Submission): Update =
    submission match {
      case AllocateParty(hint, displayName, submissionId) =>
        val party = Ref.Party.assertFromString(hint.getOrElse(UUID.randomUUID().toString))
        if (allocatedParties(party))
          logger.warn(
            s"Found duplicate party submission with ID $party for submissionId ${Some(submissionId)}"
          )(submission.loggingContext)

        allocatedParties = allocatedParties + party
        partyAllocationSuccessMapper(party, displayName, submissionId, participantId)
      case Config(maxRecordTime, submissionId, config) =>
        val recordTime = timeProvider.getCurrentTimestamp
        if (recordTime > maxRecordTime)
          Update.ConfigurationChangeRejected(
            recordTime = recordTime,
            submissionId = submissionId,
            participantId = participantId,
            proposedConfiguration = config,
            rejectionReason = s"Configuration change timed out: $maxRecordTime > $recordTime",
          )
        else {
          val expectedGeneration = ledgerConfiguration.map(_.generation).map(_ + 1L)
          if (expectedGeneration.forall(_ == config.generation)) {
            ledgerConfiguration = Some(config)
            Update.ConfigurationChanged(
              recordTime = recordTime,
              submissionId = submissionId,
              participantId = participantId,
              newConfiguration = config,
            )
          } else
            Update.ConfigurationChangeRejected(
              recordTime = recordTime,
              submissionId = submissionId,
              participantId = participantId,
              proposedConfiguration = config,
              rejectionReason =
                s"Generation mismatch: expected=$expectedGeneration, actual=${config.generation}",
            )
        }

      case other => successMapper(other, offsetIndex, participantId)
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

  private def validateParties(
      transaction: Submission.Transaction,
      transactionInformees: Set[Ref.Party],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): AsyncValidation[Unit] =
    // This check which is O(n) in the number of transaction informees does not warrant a separate async dispatch
    Future.successful {
      if (validatePartyAllocation) {
        val unallocatedInformees = transactionInformees diff allocatedParties
        Either.cond(
          unallocatedInformees.isEmpty,
          (),
          toCommandRejectedUpdate(
            UnallocatedParties(unallocatedInformees.toSet)(errorFactories),
            transaction.submitterInfo.toCompletionInfo(),
          ),
        )
      } else Right(())
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
        .lookupMaximumLedgerTime(referredContracts)(transaction.loggingContext)
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
              .lookupContractKey(transaction.transaction.informees, key)(transaction.loggingContext)
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

  private def toCommandRejectedUpdate(rejection: Rejection, completionInfo: CompletionInfo) =
    Update.CommandRejected(
      recordTime = timeProvider.getCurrentTimestamp,
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

  private def withErrorLogger[T](submissionId: Option[String])(
      f: ContextualizedErrorLogger => T
  )(implicit loggingContext: LoggingContext, logger: ContextualizedLogger) =
    f(new DamlContextualizedErrorLogger(logger, loggingContext, submissionId))
}
