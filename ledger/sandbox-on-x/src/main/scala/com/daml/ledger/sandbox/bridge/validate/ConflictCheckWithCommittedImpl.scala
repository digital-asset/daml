// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import cats.data.EitherT
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.{IndexService, MaximumLedgerTime}
import com.daml.ledger.participant.state.v2.CompletionInfo
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.bridge.validate.ConflictCheckingLedgerBridge._
import com.daml.ledger.sandbox.domain.Rejection
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.Submission.Transaction
import com.daml.lf.command.DisclosedContract
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.{Versioned, Transaction => LfTransaction}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Timed

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Conflict checking for incoming submissions against the ledger state
  * as it is visible on the Ledger API.
  */
private[validate] class ConflictCheckWithCommittedImpl(
    indexService: IndexService,
    bridgeMetrics: BridgeMetrics,
)(implicit executionContext: ExecutionContext)
    extends ConflictCheckWithCommitted {
  private[this] implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  override def apply(
      in: Validation[(Offset, PreparedSubmission)]
  ): AsyncValidation[(Offset, PreparedSubmission)] = in match {
    case Left(rejection) => Future.successful(Left(rejection))
    case Right(input @ (_, transactionSubmission: PreparedTransactionSubmission)) =>
      val submissionId = transactionSubmission.submission.submissionId

      withEnrichedLoggingContext("submissionId" -> submissionId) { implicit loggingContext =>
        withErrorLogger(Some(submissionId)) { implicit errorLogger =>
          Timed
            .future(
              bridgeMetrics.Stages.ConflictCheckWithCommitted.timer,
              validate(transactionSubmission).map(_.map(_ => input)),
            )
        }
      }(transactionSubmission.submission.loggingContext)

    case Right(validated) => Future.successful(Right(validated))
  }

  private def validate(
      inputSubmission: PreparedTransactionSubmission
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger,
      loggingContext: LoggingContext,
  ): Future[Either[Rejection, Unit]] = {
    import inputSubmission._

    val eitherTF: EitherT[Future, Rejection, Unit] =
      for {
        _ <- validateExplicitDisclosure(
          submission.disclosedContracts,
          submission.submitterInfo.toCompletionInfo(),
        )
        _ <- validateCausalMonotonicity(
          transaction = submission,
          inputContracts = inputContracts,
          transactionLedgerEffectiveTime = submission.transactionMeta.ledgerEffectiveTime,
          divulged = blindingInfo.divulgence.keySet,
        )
        _ <- validateKeyUsages(
          transactionInformees,
          keyInputs,
          submission.loggingContext,
          submission.submitterInfo.toCompletionInfo(),
        )
      } yield ()

    eitherTF.value
  }

  private def validateCausalMonotonicity(
      transaction: Transaction,
      inputContracts: Set[ContractId],
      transactionLedgerEffectiveTime: Timestamp,
      divulged: Set[ContractId],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): EitherT[Future, Rejection, Unit] = {
    val referredContracts = inputContracts.diff(divulged)
    val completionInfo = transaction.submitterInfo.toCompletionInfo()

    if (referredContracts.isEmpty)
      EitherT[Future, Rejection, Unit](Future.successful(Right(())))
    else
      EitherT(
        indexService
          .lookupMaximumLedgerTimeAfterInterpretation(referredContracts)(transaction.loggingContext)
          .transform {
            case Success(MaximumLedgerTime.Archived(missingContractIds)) =>
              Success(Left(UnknownContracts(missingContractIds)(completionInfo)))

            case Failure(err) =>
              Success(Left(LedgerBridgeInternalError(err, completionInfo)))

            case Success(MaximumLedgerTime.Max(maximumLedgerEffectiveTime))
                if maximumLedgerEffectiveTime > transactionLedgerEffectiveTime =>
              Success(
                Left(
                  CausalMonotonicityViolation(
                    contractLedgerEffectiveTime = maximumLedgerEffectiveTime,
                    transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
                  )(completionInfo)
                )
              )

            case Success(_) => Success(Right(()))
          }
      )
  }

  private def validateKeyUsages(
      transactionInformees: Set[Ref.Party],
      keyInputs: KeyInputs,
      loggingContext: LoggingContext,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): EitherT[Future, Rejection, Unit] = {
    keyInputs.foldLeft(EitherT(Future.successful[Validation[Unit]](Right(())))) {
      case (f, (key, inputState)) =>
        f.flatMapF { _ =>
          indexService
            .lookupContractKey(transactionInformees, key)(loggingContext)
            .map { lookupResult =>
              (inputState, lookupResult) match {
                case (LfTransaction.NegativeKeyLookup, Some(actual)) =>
                  Left(InconsistentContractKey(None, Some(actual))(completionInfo))
                case (LfTransaction.KeyCreate, Some(_)) =>
                  Left(DuplicateKey(key)(completionInfo))
                case (LfTransaction.KeyActive(expected), actual) if !actual.contains(expected) =>
                  Left(InconsistentContractKey(Some(expected), actual)(completionInfo))
                case _ => Right(())
              }
            }
        }
    }
  }

  private def validateExplicitDisclosure(
      disclosedContracts: ImmArray[Versioned[DisclosedContract]],
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger,
      loggingContext: LoggingContext,
  ): EitherT[Future, Rejection, Unit] = {
    // Note that the validation fails fast on the first unknown/invalid contract.
    disclosedContracts.foldLeft(EitherT(Future.successful[Validation[Unit]](Right(())))) {
      case (f, provided) =>
        f.flatMapF { _ =>
          indexService
            .lookupContractForValidation(provided.unversioned.contractId)
            .map {
              case None =>
                // Disclosed contract was archived or never existed
                Left(UnknownContracts(Set(provided.unversioned.contractId))(completionInfo))
              case Some(actual) =>
                sameContractData(actual, provided).left.map { errMessage =>
                  logger.info(errMessage)
                  DisclosedContractInvalid(provided.unversioned.contractId, completionInfo)
                }
            }
        }
    }
  }

  private def sameContractData(
      actual: (Value.VersionedContractInstance, Timestamp),
      provided: Versioned[DisclosedContract],
  ): Either[String, Unit] = {
    val providedContractId = provided.unversioned.contractId

    val actualTemplate = actual._1.unversioned.template
    val providedTemplate = provided.unversioned.templateId

    val actualArgument = actual._1.unversioned.arg
    val providedArgument = provided.unversioned.argument

    val actualLet = actual._2
    val providedLet = provided.unversioned.metadata.createdAt

    if (actualTemplate != providedTemplate)
      Left(s"Disclosed contract $providedContractId has invalid template id")
    else if (actualArgument != providedArgument)
      Left(s"Disclosed contract $providedContractId has invalid argument")
    else if (actualLet != providedLet)
      Left(s"Disclosed contract $providedContractId has invalid ledgerEffectiveTime")
    else
      Right(())
  }
}
