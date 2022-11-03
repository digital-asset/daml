// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.error

import com.daml.error.{ContextualizedErrorLogger, DamlError}
import com.daml.lf.engine.Error.{Interpretation, Package, Preprocessing, Validation}
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.error.definitions.LedgerApiErrors

sealed abstract class ErrorCause extends Product with Serializable

object ErrorCause {
  final case class DamlLf(error: LfError) extends ErrorCause
  final case class LedgerTime(retries: Int) extends ErrorCause
}

object RejectionGenerators {

  def commandExecutorError(cause: ErrorCause)(implicit
      errorLoggingContext: ContextualizedErrorLogger
  ): DamlError = {

    def processPackageError(err: LfError.Package.Error): DamlError = err match {
      case e: Package.Internal => LedgerApiErrors.InternalError.Package(e.message)
      case Package.Validation(validationError) =>
        LedgerApiErrors.CommandExecution.Package.PackageValidationFailed
          .Reject(validationError.pretty)
      case Package.MissingPackage(packageId, context) =>
        LedgerApiErrors.RequestValidation.NotFound.Package
          .InterpretationReject(packageId, context)
      case Package.AllowedLanguageVersion(packageId, languageVersion, allowedLanguageVersions) =>
        LedgerApiErrors.CommandExecution.Package.AllowedLanguageVersions.Error(
          packageId,
          languageVersion,
          allowedLanguageVersions,
        )
      case e: Package.SelfConsistency =>
        LedgerApiErrors.InternalError.Package(e.message)
    }

    def processPreprocessingError(err: LfError.Preprocessing.Error): DamlError = err match {
      case e: Preprocessing.Internal => LedgerApiErrors.InternalError.Package(e.message)
      case e => LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed.Reject(e)
    }

    def processValidationError(err: LfError.Validation.Error): DamlError = err match {
      // we shouldn't see such errors during submission
      case e: Validation.ReplayMismatch => LedgerApiErrors.InternalError.Validation(e.message)
    }

    def processDamlException(
        err: com.daml.lf.interpretation.Error,
        renderedMessage: String,
        detailMessage: Option[String],
    ): DamlError = {
      // detailMessage is only suitable for server side debugging but not for the user, so don't pass except on internal errors

      err match {
        case LfInterpretationError.ContractNotFound(cid) =>
          LedgerApiErrors.ConsistencyErrors.ContractNotFound
            .Reject(renderedMessage, cid)
        case LfInterpretationError.ContractKeyNotFound(key) =>
          LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound
            .Reject(renderedMessage, key)
        case _: LfInterpretationError.FailedAuthorization =>
          LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError
            .Reject(renderedMessage)
        case e: LfInterpretationError.ContractNotActive =>
          LedgerApiErrors.CommandExecution.Interpreter.ContractNotActive
            .Reject(renderedMessage, e)
        case _: LfInterpretationError.DisclosedContractKeyHashingError =>
          LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError
            .Error(renderedMessage)
        case _: LfInterpretationError.ContractKeyNotVisible =>
          LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError
            .Error(renderedMessage)
        case LfInterpretationError.DuplicateContractKey(key) =>
          LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
            .RejectWithContractKeyArg(renderedMessage, key)
        case LfInterpretationError.InconsistentContractKey(key) =>
          LedgerApiErrors.ConsistencyErrors.InconsistentContractKey
            .RejectWithContractKeyArg(renderedMessage, key)
        case _: LfInterpretationError.UnhandledException =>
          LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError.Error(
            renderedMessage + detailMessage.fold("")(x => ". Details: " + x)
          )
        case _: LfInterpretationError.UserError =>
          LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError
            .Error(renderedMessage)
        case _: LfInterpretationError.TemplatePreconditionViolated =>
          LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError
            .Error(renderedMessage)
        case _: LfInterpretationError.CreateEmptyContractKeyMaintainers =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case _: LfInterpretationError.FetchEmptyContractKeyMaintainers =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case _: LfInterpretationError.WronglyTypedContract =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case _: LfInterpretationError.ContractDoesNotImplementInterface =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case _: LfInterpretationError.ContractDoesNotImplementRequiringInterface =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case LfInterpretationError.NonComparableValues =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case _: LfInterpretationError.ContractIdInContractKey =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case LfInterpretationError.Limit(_) =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case _: LfInterpretationError.ContractIdComparability =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case _: LfInterpretationError.ChoiceGuardFailed =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case _: LfInterpretationError.DisclosurePreprocessing =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
      }
    }

    def processInterpretationError(
        err: LfError.Interpretation.Error,
        detailMessage: Option[String],
    ): DamlError =
      err match {
        case Interpretation.Internal(location, message, _) =>
          LedgerApiErrors.InternalError.Interpretation(location, message, detailMessage)
        case m @ Interpretation.DamlException(error) =>
          processDamlException(error, m.message, detailMessage)
      }

    def processLfError(error: LfError) = {
      val transformed = error match {
        case LfError.Package(packageError) => processPackageError(packageError)
        case LfError.Preprocessing(processingError) => processPreprocessingError(processingError)
        case LfError.Interpretation(interpretationError, detailMessage) =>
          processInterpretationError(interpretationError, detailMessage)
        case LfError.Validation(validationError) => processValidationError(validationError)
        case e
            if e.message.contains(
              "requires authorizers"
            ) => // Keeping this around as a string match as daml is not yet generating LfError.InterpreterErrors.Validation
          LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError.Reject(e.message)
      }
      transformed
    }

    cause match {
      case ErrorCause.DamlLf(error) => processLfError(error)
      case x: ErrorCause.LedgerTime =>
        LedgerApiErrors.CommandExecution.FailedToDetermineLedgerTime
          .Reject(s"Could not find a suitable ledger time after ${x.retries} retries")
    }
  }
}
