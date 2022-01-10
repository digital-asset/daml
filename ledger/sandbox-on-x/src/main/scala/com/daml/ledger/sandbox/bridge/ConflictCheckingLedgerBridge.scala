// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.{CompletionInfo, Update}
import com.daml.ledger.sandbox.bridge.ConflictCheckingLedgerBridge._
import com.daml.ledger.sandbox.domain.Rejection._
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
import scala.util.{Failure, Success, Try}

private[sandbox] class ConflictCheckingLedgerBridge(
    indexService: IndexService,
    sequence: Sequence,
    bridgeMetrics: BridgeMetrics,
    errorFactories: ErrorFactories,
    servicesThreadPoolSize: Int,
)(implicit
    servicesExecutionContext: ExecutionContext
) extends LedgerBridge {
  private[this] implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  def flow: Flow[Submission, (Offset, Update), NotUsed] =
    Flow[Submission]
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
        Future {
          transaction.transaction.contractKeyInputs
            .map(contractKeyInputs => {
              PreparedTransactionSubmission(
                contractKeyInputs,
                transaction.transaction.inputContracts,
                transaction.transaction.updatedContractKeys,
                transaction.transaction.consumedContracts,
                Blinding.blind(transaction),
                transaction.informees,
                transactionSubmission,
              )
            })
            .left
            .map(
              withErrorLogger(submitterInfo.submissionId)(
                invalidInputFromParticipantRejection(submitterInfo.toCompletionInfo())(_)
              )(transactionSubmission.loggingContext, logger)
            )
        },
      )
    case other => Future.successful(Right(NoOpPreparedSubmission(other)))
  }

  // Tags the prepared submission with the current ledger end as available on the Ledger API.
  private[bridge] val tagWithLedgerEnd: TagWithLedgerEnd = {
    case Left(rejection) => Future.successful(Left(rejection))
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
  private[bridge] val conflictCheckWithCommitted: ConflictCheckWithCommitted = {
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
      implicit val submissionLoggingContext: LoggingContext = originalSubmission.loggingContext
      withErrorLogger(originalSubmission.submitterInfo.submissionId) { implicit errorLogger =>
        val completionInfo = originalSubmission.submitterInfo.toCompletionInfo()
        Timed
          .future(
            bridgeMetrics.Stages.conflictCheckWithCommitted,
            validateCausalMonotonicity(
              completionInfo = completionInfo,
              inputContracts = inputContracts,
              transactionLedgerEffectiveTime =
                originalSubmission.transactionMeta.ledgerEffectiveTime,
              divulged = blindingInfo.divulgence.keySet,
              loggingContext = submissionLoggingContext,
            ).flatMap {
              case Right(_) =>
                validateKeyUsages(
                  transactionInformees,
                  completionInfo,
                  keyInputs,
                  submissionLoggingContext,
                )
              case rejection => Future.successful(rejection)
            },
          )
          .map(_.map(_ => validated))
      }
    case Right(validated) => Future.successful(Right(validated))
  }

  private def validateCausalMonotonicity(
      completionInfo: CompletionInfo,
      inputContracts: Set[ContractId],
      transactionLedgerEffectiveTime: Timestamp,
      divulged: Set[ContractId],
      loggingContext: LoggingContext,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): AsyncValidation[Unit] = {
    val referredContracts = inputContracts.diff(divulged)
    if (referredContracts.isEmpty)
      Future.successful(Right(()))
    else
      indexService
        .lookupMaximumLedgerTime(referredContracts)(loggingContext)
        .transform {
          case Failure(MissingContracts(missingContractIds)) =>
            Success(Left(UnknownContracts(missingContractIds)(completionInfo, errorFactories)))
          case Failure(err) =>
            Success(Left(LedgerBridgeInternalError(err, completionInfo)))
          case Success(maximumLedgerEffectiveTime) =>
            maximumLedgerEffectiveTime
              .filter(_ > transactionLedgerEffectiveTime)
              .fold[Try[Validation[Unit]]](Success(Right(())))(contractLedgerEffectiveTime =>
                Success(
                  Left(
                    CausalMonotonicityViolation(
                      contractLedgerEffectiveTime = contractLedgerEffectiveTime,
                      transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
                    )(completionInfo, errorFactories)
                  )
                )
              )
        }
  }

  private def validateKeyUsages(
      transactionInformees: Set[Ref.Party],
      completionInfo: CompletionInfo,
      keyInputs: KeyInputs,
      loggingContext: LoggingContext,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): AsyncValidation[Unit] =
    keyInputs.foldLeft(Future.successful[Validation[Unit]](Right(()))) {
      case (f, (key, inputState)) =>
        f.flatMap {
          case Right(_) =>
            indexService
              // TODO SoX: Perform lookup more efficiently and do not use a readers-based lookup
              .lookupContractKey(transactionInformees, key)(loggingContext)
              .map { lookupResult =>
                (inputState, lookupResult) match {
                  case (LfTransaction.NegativeKeyLookup, Some(actual)) =>
                    Left(
                      InconsistentContractKey(None, Some(actual))(completionInfo, errorFactories)
                    )
                  case (LfTransaction.KeyCreate, Some(_)) =>
                    Left(DuplicateKey(key)(completionInfo, errorFactories))
                  case (LfTransaction.KeyActive(expected), actual) if !actual.contains(expected) =>
                    Left(
                      InconsistentContractKey(Some(expected), actual)(
                        completionInfo,
                        errorFactories,
                      )
                    )
                  case _ => Right(())
                }
              }
          case left => Future.successful(left)
        }
    }

  private def invalidInputFromParticipantRejection(completionInfo: CompletionInfo)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): LfTransaction.KeyInputError => Rejection = {
    case LfTransaction.InconsistentKeys(key) =>
      TransactionInternallyInconsistentKey(key, completionInfo)
    case LfTransaction.DuplicateKeys(key) =>
      TransactionInternallyInconsistentContract(key, completionInfo)
  }
}

object ConflictCheckingLedgerBridge {
  private[bridge] type Validation[T] = Either[Rejection, T]
  private type AsyncValidation[T] = Future[Validation[T]]
  private[bridge] type KeyInputs = Map[Key, LfTransaction.KeyInput]

  // Conflict checking stages
  private type PrepareSubmission = Submission => AsyncValidation[PreparedSubmission]
  private type TagWithLedgerEnd =
    Validation[PreparedSubmission] => AsyncValidation[(Offset, PreparedSubmission)]
  private type ConflictCheckWithCommitted =
    Validation[(Offset, PreparedSubmission)] => AsyncValidation[(Offset, PreparedSubmission)]
  private[bridge] type Sequence =
    () => Validation[(Offset, PreparedSubmission)] => Iterable[(Offset, Update)]

  private[bridge] def withErrorLogger[T](submissionId: Option[String])(
      f: ContextualizedErrorLogger => T
  )(implicit loggingContext: LoggingContext, logger: ContextualizedLogger) =
    f(new DamlContextualizedErrorLogger(logger, loggingContext, submissionId))
}
