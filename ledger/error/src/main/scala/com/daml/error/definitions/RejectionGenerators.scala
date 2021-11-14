// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error.{BaseError, ContextualizedErrorLogger, ErrorCause, ErrorCode}
import com.daml.lf.engine.Error.{Interpretation, Package, Preprocessing, Validation}
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

class RejectionGenerators(conformanceMode: Boolean) {
  // TODO error codes: Remove conformance mode
  private val adjustErrors = Map(
    LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound -> Code.INVALID_ARGUMENT,
    LedgerApiErrors.CommandExecution.Interpreter.ContractNotActive -> Code.INVALID_ARGUMENT,
    LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractNotFound -> Code.ABORTED,
    LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound -> Code.INVALID_ARGUMENT,
    LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError -> Code.INVALID_ARGUMENT,
  )

  private def enforceConformance(ex: StatusRuntimeException): StatusRuntimeException =
    if (!conformanceMode) ex
    else {
      adjustErrors
        .find { case (k, _) =>
          ex.getStatus.getDescription.startsWith(k.id + "(")
        }
        .fold(ex) { case (_, newGrpcCode) =>
          val parsed = StatusProto.fromThrowable(ex)
          // rewrite status to use "conformance" code
          val bld = com.google.rpc.Status
            .newBuilder()
            .setCode(newGrpcCode.value())
            .setMessage(parsed.getMessage)
            .addAllDetails(parsed.getDetailsList)
          val newEx = StatusProto.toStatusRuntimeException(bld.build())
          // strip stack trace from exception
          new ErrorCode.ApiException(newEx.getStatus, newEx.getTrailers)
        }
    }

  def toGrpc(reject: BaseError)(implicit
      errorLoggingContext: ContextualizedErrorLogger
  ): StatusRuntimeException =
    enforceConformance(reject.asGrpcErrorFromContext)

  def commandExecutorError(cause: ErrorCauseExport)(implicit
      errorLoggingContext: ContextualizedErrorLogger
  ): StatusRuntimeException = {

    def processPackageError(err: LfError.Package.Error): BaseError = err match {
      case e: Package.Internal => LedgerApiErrors.InternalError.PackageInternal(e)
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
        LedgerApiErrors.InternalError.PackageSelfConsistency(e)
    }

    def processPreprocessingError(err: LfError.Preprocessing.Error): BaseError = err match {
      case e: Preprocessing.Internal => LedgerApiErrors.InternalError.Preprocessing(e)
      case e => LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed.Reject(e)
    }

    def processValidationError(err: LfError.Validation.Error): BaseError = err match {
      // we shouldn't see such errors during submission
      case e: Validation.ReplayMismatch => LedgerApiErrors.InternalError.Validation(e)
    }

    def processDamlException(
        err: com.daml.lf.interpretation.Error,
        renderedMessage: String,
        detailMessage: Option[String],
    ): BaseError = {
      // detailMessage is only suitable for server side debugging but not for the user, so don't pass except on internal errors

      err match {
        case LfInterpretationError.ContractNotFound(cid) =>
          LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractNotFound
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
        case _: LfInterpretationError.LocalContractKeyNotVisible =>
          LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError
            .Error(renderedMessage)
        case LfInterpretationError.DuplicateContractKey(key) =>
          LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
            .InterpretationReject(renderedMessage, key)
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
        case LfInterpretationError.ValueExceedsMaxNesting =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
        case _: LfInterpretationError.ContractIdComparability =>
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError
            .Error(
              renderedMessage
            )
      }
    }

    def processInterpretationError(
        err: LfError.Interpretation.Error,
        detailMessage: Option[String],
    ): BaseError =
      err match {
        case Interpretation.Internal(location, message) =>
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
      toGrpc(transformed)
    }

    cause match {
      case ErrorCauseExport.DamlLf(error) => processLfError(error)
      case x: ErrorCauseExport.LedgerTime =>
        toGrpc(
          LedgerApiErrors.CommandExecution.FailedToDetermineLedgerTime
            .Reject(x.explain)
        )
    }
  }
}

// TODO error codes: Remove with the removal of the compatibility constraint from Canton
object RejectionGenerators extends RejectionGenerators(conformanceMode = false)

sealed trait ErrorCauseExport
object ErrorCauseExport {
  final case class DamlLf(error: LfError) extends ErrorCauseExport
  final case class LedgerTime(retries: Int, explain: String) extends ErrorCauseExport

  def fromErrorCause(err: ErrorCause): ErrorCauseExport = err match {
    case ErrorCause.DamlLf(error) => DamlLf(error)
    case x: ErrorCause.LedgerTime => LedgerTime(x.retries, x.explain)
  }
}
