package com.daml.platform.apiserver.error

import com.daml.ledger.api.domain
import com.daml.ledger.participant.state
import com.daml.lf.engine.Error.{Interpretation, Package, Preprocessing, Validation}
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException

import scala.util.{Failure, Success, Try}

object RejectionGenerators {

  private[sync] def toGrpc(reject: BaseError): StatusRuntimeException = {
    reject.asGrpcErrorFromContext(this.loggingContext)
  }

  override def duplicateCommand(commands: domain.Commands): StatusRuntimeException = {
    toGrpc(LedgerApiErrors.CommandPreparation.DuplicateCommand.Reject()(this.loggingContext))
  }

  override def commandExecutorError(commands: domain.Commands,
                                    cause: ErrorCauseExport): Option[StatusRuntimeException] = {

    def processPackageError(err: LfError.Package.Error): BaseError = err match {
      case e: Package.Internal => LedgerApiErrors.InternalError.PackageInternal(e)
      case Package.Validation(validationError) =>
        LedgerApiErrors.Package.PackageValidationFailed.Reject(validationError)
      case Package.MissingPackage(packageId, context) =>
        LedgerApiErrors.Package.MissingPackage.Reject(packageId, context)
      case Package.AllowedLanguageVersion(packageId, languageVersion, allowedLanguageVersions) =>
        LedgerApiErrors.Package.AllowedLanguageVersions.Error(packageId, languageVersion, allowedLanguageVersions)
      case e: Package.SelfConsistency =>
        LedgerApiErrors.InternalError.PackageSelfConsistency(e)
    }

    def processPreprocessingError(err: LfError.Preprocessing.Error): BaseError = err match {
      case e: Preprocessing.Internal => LedgerApiErrors.InternalError.Preprocessing(e)
      case e                         => LedgerApiErrors.PreprocessingErrors.PreprocessingFailed.Reject(e)
    }

    def processValidationError(err: LfError.Validation.Error): BaseError = err match {
      // we shouldn't see such errors during submission
      case e: Validation.ReplayMismatch => LedgerApiErrors.InternalError.Validation(e)
    }

    def processDamlException(err: com.daml.lf.interpretation.Error, renderedMessage: String): BaseError = {
      // detailMessage is only suitable for server side debugging but not for the user, so don't pass except on internal errors

      err match {
        case LfInterpretationError.ContractNotFound(cid) =>
          LedgerApiErrors.InterpreterErrors.LookupErrors.ContractNotFound.Reject(renderedMessage, cid)
        case LfInterpretationError.ContractKeyNotFound(key) =>
          LedgerApiErrors.InterpreterErrors.LookupErrors.ContractKeyNotFound.Reject(renderedMessage, key)
        case e: LfInterpretationError.FailedAuthorization =>
          LedgerApiErrors.InterpreterErrors.AuthorizationError.Reject(renderedMessage)
        case e: LfInterpretationError.ContractNotActive =>
          LedgerApiErrors.InterpreterErrors.ContractNotActive.Reject(renderedMessage, e)
        case e: LfInterpretationError.LocalContractKeyNotVisible =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
        case LfInterpretationError.DuplicateContractKey(key) =>
          LedgerApiErrors.InterpreterErrors.DuplicateContractKey.Reject(renderedMessage, key)
        case e: LfInterpretationError.UnhandledException =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
        case e: LfInterpretationError.UserError =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
        case e: LfInterpretationError.TemplatePreconditionViolated =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
        case e: LfInterpretationError.CreateEmptyContractKeyMaintainers =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
        case e: LfInterpretationError.FetchEmptyContractKeyMaintainers =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
        case e: LfInterpretationError.WronglyTypedContract =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
        case LfInterpretationError.NonComparableValues =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
        case e: LfInterpretationError.ContractIdInContractKey =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
        case LfInterpretationError.ValueExceedsMaxNesting =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
        case e: LfInterpretationError.ContractIdComparability =>
          LedgerApiErrors.InterpreterErrors.GenericInterpretationError.Error(renderedMessage)
      }
    }

    def processInterpretationError(err: LfError.Interpretation.Error, detailMessage: Option[String]): BaseError =
      err match {
        case Interpretation.Internal(location, message) =>
          LedgerApiErrors.InternalError.Interpretation(location, message, detailMessage)
        case m @ Interpretation.DamlException(error) => processDamlException(error, m.message)
      }

    def processLfError(error: LfError) = {
      val transformed = error match {
        case LfError.Package(packageError)          => processPackageError(packageError)
        case LfError.Preprocessing(processingError) => processPreprocessingError(processingError)
        case LfError.Interpretation(interpretationError, detailMessage) =>
          processInterpretationError(interpretationError, detailMessage)
        case LfError.Validation(validationError) => processValidationError(validationError)
        case e
          if e.message.contains("requires authorizers") => // Keeping this around as a string match as daml is not yet generating LfError.InterpreterErrors.Validation
          LedgerApiErrors.InterpreterErrors.AuthorizationError.Reject(e.message)
      }
      toGrpc(transformed)
    }

    val rej = cause match {
      case ErrorCauseExport.DamlLf(error) => processLfError(error)
      case x: ErrorCauseExport.LedgerTime =>
        toGrpc(LedgerApiErrors.CommandPreparation.FailedToDetermineLedgerTime.Reject(x.explain))
    }
    Some(rej)
  }

  override def submissionResult(commands: domain.Commands,
                                result: Try[state.v2.SubmissionResult]): Option[Try[Unit]] = {
    result match {
      case Success(state.v2.SubmissionResult.Acknowledged)                    => None
      case Success(grpcError @ state.v2.SubmissionResult.SynchronousError(_)) => Some(Failure(grpcError.exception))
      case Failure(_)                                                         => None
    }
  }

  override def validationFailure(reject: StatusRuntimeException): StatusRuntimeException = {
    implicit val errorContext: ErrorLoggingContext = this.loggingContext(TraceContext.empty)
    val description                                = reject.getStatus.getDescription
    reject.getStatus.getCode match {
      case Code.INVALID_ARGUMENT =>
        if (description.startsWith("Missing field:")) {
          toGrpc(LedgerApiErrors.CommandValidation.MissingField.Reject(description))
        } else if (description.startsWith("Invalid argument:")) {
          toGrpc(LedgerApiErrors.CommandValidation.InvalidArgument.Reject(description))
        } else if (description.startsWith("Invalid field:")) {
          toGrpc(LedgerApiErrors.CommandValidation.InvalidField.Reject(description))
        } else {
          noTracingLogger.warn(s"Unknown invalid argument rejection: ${reject.getStatus}")
          reject
        }
      case Code.NOT_FOUND if description.startsWith("Ledger ID") =>
        toGrpc(LedgerApiErrors.CommandValidation.LedgerIdMismatch.Reject(description))
      case x =>
        noTracingLogger.warn(s"Unknown rejection: ${reject.getStatus}")
        reject
    }
  }
}
