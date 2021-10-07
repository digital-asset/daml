package com.daml.error.definitions

import com.daml.error._
import com.daml.error.definitions.ErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LedgerApiErrorGroup
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Error.Validation.ReplayMismatch
import com.daml.lf.language.{LanguageVersion, LookupError, Reference}
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.{VersionRange, language}
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}

object LedgerApiErrors extends LedgerApiErrorGroup {

  // the authorization checks are here only for documentation purpose.
  // TODO error codes: Extract these errors in ledger-api-auth and use them in [[com.daml.ledger.api.auth.Authorizer]]
  //                   (i.e. in lieu of ErrorFactories.permissionDenied() and ErrorFactories.unauthenticated())
  object AuthorizationChecks extends ErrorGroup() {

    @Explanation(
      """This rejection is given if the submitted command does not contain a JWT token on a participant enforcing JWT authentication."""
    )
    @Resolution("Ask your participant operator to provide you with an appropriate JWT token.")
    object Unauthenticated
        extends ErrorCode(
          id = "UNAUTHENTICATED",
          ErrorCategory.AuthInterceptorInvalidAuthenticationCredentials,
        ) {
      case class Reject()(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = "The command is missing a JWT token"
          )
    }

    @Explanation(
      """This rejection is given if the supplied JWT token is not sufficient for the intended command.
        |The exact reason is logged on the participant, but not given to the user for security reasons."""
    )
    @Resolution(
      "Inspect your command and your token, or ask your participant operator for an explanation why this command failed."
    )
    object PermissionDenied
        extends ErrorCode(id = "PERMISSION_DENIED", ErrorCategory.InsufficientPermission) {
      case class Reject()(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = "The provided JWT token is not sufficient to authorize the intended command"
          )
    }
  }

  object CommandValidation extends ErrorGroup {
    @Explanation(
      """Every ledger Api command contains a ledger-id which is verifying against the running ledger.
          This error indicates that the provided ledger-id does not match the expected one."""
    )
    @Resolution("Ensure that your application is correctly configured to use the correct ledger.")
    object LedgerIdMismatch
        extends ErrorCode(
          id = "LEDGER_ID_MISMATCH",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      case class Reject(override val cause: String)(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = cause
          )
    }

    @Explanation(
      """This error is emitted, when a submitted ledger Api command could not be successfully deserialized due to mandatory fields not being set."""
    )
    @Resolution("Inspect the reason given and correct your application.")
    object MissingField
        extends ErrorCode(id = "MISSING_FIELDS", ErrorCategory.InvalidIndependentOfSystemState) {
      case class Reject(_reason: String)(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = s"The submitted command is missing a mandatory field: ${_reason}"
          )
    }

    @Explanation(
      """This error is emitted, when a submitted ledger Api command contained an invalid argument."""
    )
    @Resolution("Inspect the reason given and correct your application.")
    object InvalidArgument
        extends ErrorCode(id = "INVALID_ARGUMENT", ErrorCategory.InvalidIndependentOfSystemState) {
      case class Reject(_reason: String)(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = s"The submitted command has invalid arguments: ${_reason}"
          )
    }

    @Explanation(
      """This error is emitted, when a submitted ledger Api command contained a field value that could not be properly understood"""
    )
    @Resolution("Inspect the reason given and correct your application.")
    object InvalidField
        extends ErrorCode(id = "INVALID_FIELD", ErrorCategory.InvalidIndependentOfSystemState) {
      case class Reject(_reason: String)(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = s"The submitted command has a field with invalid value: ${_reason}"
          )
    }

  }

  object CommandPreparation extends ErrorGroup {

    @Explanation("Command deduplication")
    @Resolution("Celebrate, as your command has already been delivered")
    object DuplicateCommand
        extends ErrorCode(
          id = "DUPLICATE_COMMAND",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
        ) {

      case class Reject()(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = "A command with the given command id has already been successfully processed"
          )
    }

    @Explanation(
      """This error occurs if the participant fails to determine the max ledger time of the used 
        |contracts. Most likely, this means that one of the contracts is not active anymore which can 
        |happen under contention. However, it can also happen with contract keys.
        |"""
    )
    @Resolution("Retry the transaction")
    object FailedToDetermineLedgerTime
        extends ErrorCode(
          id = "FAILED_TO_DETERMINE_LEDGER_TIME",
          ErrorCategory.ContentionOnSharedResources,
        ) {

      case class Reject(_reason: String)(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause =
              s"The participant failed to determine the max ledger time for this command: ${_reason}"
          )

    }
  }

  object Package extends ErrorGroup() {

    @Explanation(
      """This error indicates that the uploaded dar is based on an unsupported language version."""
    )
    @Resolution("Use a Dar compiled with a language version that this participant supports.")
    object AllowedLanguageVersions
        extends ErrorCode(
          id = "ALLOWED_LANGUAGE_VERSIONS",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      def buildCause(
          packageId: PackageId,
          languageVersion: LanguageVersion,
          allowedLanguageVersions: VersionRange[LanguageVersion],
      ): String =
        LfError.Package
          .AllowedLanguageVersion(packageId, languageVersion, allowedLanguageVersions)
          .message

      case class Error(
          packageId: Ref.PackageId,
          languageVersion: language.LanguageVersion,
          allowedLanguageVersions: VersionRange[language.LanguageVersion],
      )(implicit
          val loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = buildCause(packageId, languageVersion, allowedLanguageVersions)
          )
    }

    @Explanation(
      """This error occurs if the Daml transaction is referring to a package which is not known to the participant."""
    )
    @Resolution("Upload the necessary Dars to the participant node.")
    object MissingPackage
        extends ErrorCode(
          id = "MISSING_PACKAGE",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      case class Reject(
          packageId: PackageId,
          reference: Reference,
      )(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = LookupError.MissingPackage.pretty(packageId, reference)
          )

    }

    @Explanation(
      """This error occurs if a package referred to by a Command fails validation. This should not happen as packages are validated when being uploaded."""
    )
    @Resolution("Contact support.")
    object PackageValidationFailed
        extends ErrorCode(
          id = "PACKAGE_VALIDATION_FAILED",
          ErrorCategory.MaliciousOrFaultyBehaviour,
        ) {
      case class Reject(validationErrorCause: String)(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = validationErrorCause
          )
    }
  }

  object PreprocessingErrors extends ErrorGroup {
    @Explanation("""This error occurs if a command fails during interpreter pre-processing.""")
    @Resolution("Inspect error details and correct your application.")
    object PreprocessingFailed
        extends ErrorCode(
          id = "COMMAND_PREPROCESSING_FAILED",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      case class Reject(
          err: LfError.Preprocessing.Error
      )(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = err.message
          )
    }
  }

  object InterpreterErrors extends ErrorGroup {

    @Explanation("""This error occurs if the Daml transaction failed during interpretation.""")
    @Resolution("This error type occurs if there is an application error.")
    object GenericInterpretationError
        extends ErrorCode(
          id = "DAML_INTERPRETATION_ERROR",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      case class Error(override val cause: String)(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = cause
          )

    }

    @Explanation(
      """This error occurs if the Daml transaction failed during interpretation due to an invalid argument."""
    )
    @Resolution("This error type occurs if there is an application error.")
    object InvalidArgumentInterpretationError
        extends ErrorCode(
          id = "DAML_INTERPRETER_INVALID_ARGUMENT",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      case class Error(override val cause: String)(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = cause
          )

    }

    @Explanation(
      """This error occurs if an exercise or fetch happens on a transaction-locally consumed contract."""
    )
    @Resolution("This error indicates an application error.")
    object ContractNotActive
        extends ErrorCode(
          id = "CONTRACT_NOT_ACTIVE",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {

      case class Reject(
          override val cause: String,
          _err: LfInterpretationError.ContractNotActive,
      )(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = cause
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          (ErrorResource.ContractId, _err.coid.coid)
        )
      }

    }

    @Explanation(
      """This error signals that within the transaction we got to a point where two contracts with the same key were active."""
    )
    @Resolution("This error indicates an application error.")
    object DuplicateContractKey
        extends ErrorCode(
          id = "DUPLICATE_CONTRACT_KEY_DURING_INTERPRETATION",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
        ) {
      case class Reject(
          override val cause: String,
          _key: GlobalKey,
      )(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = cause
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          // TODO error codes: Reconsider the transport format for the contract key.
          //                   If the key is big, it can force chunking other resources.
          (ErrorResource.ContractKey, _key.toString())
        )
      }
    }

    object LookupErrors extends ErrorGroup {

      @Explanation("""This error occurs if the Damle interpreter can not find a referenced contract. This
          |can be caused by either the contract not being known to the participant, or not being known to
          |the submitting parties or already being archived.""")
      @Resolution("This error type occurs if there is contention on a contract.")
      object ContractNotFound
          extends ErrorCode(
            id = "CONTRACT_NOT_FOUND",
            ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
          ) {

        case class Reject(
            override val cause: String,
            _cid: Value.ContractId,
        )(implicit
            loggingContext: ErrorCodeLoggingContext
        ) extends LoggingTransactionErrorImpl(
              cause = cause
            ) {
          override def resources: Seq[(ErrorResource, String)] = Seq(
            (ErrorResource.ContractId, _cid.coid)
          )
        }

      }

      @Explanation(
        """This error occurs if the Damle interpreter can not resolve a contract key to an active contract. This
          |can be caused by either the contract key not being known to the participant, or not being known to
          |the submitting parties or the contract representing the key has already being archived."""
      )
      @Resolution("This error type occurs if there is contention on a contract.")
      object ContractKeyNotFound
          extends ErrorCode(
            id = "CONTRACT_KEY_NOT_FOUND",
            ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
          ) {

        case class Reject(
            override val cause: String,
            _key: GlobalKey,
        )(implicit
            loggingContext: ErrorCodeLoggingContext
        ) extends LoggingTransactionErrorImpl(
              cause = cause
            ) {
          override def resources: Seq[(ErrorResource, String)] = Seq(
            (ErrorResource.ContractKey, _key.toString())
          )
        }

      }

    }

    @Explanation("""This error occurs if the Daml transaction fails due to an authorization error.
        |An authorization means that the Daml transaction computed a different set of required submitters than
        |you have provided during the submission as `actAs` parties.""")
    @Resolution("This error type occurs if there is an application error.")
    object AuthorizationError
        extends ErrorCode(
          id = "DAML_AUTHORIZATION_ERROR",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      case class Reject(override val cause: String)(implicit
          loggingContext: ErrorCodeLoggingContext
      ) extends LoggingTransactionErrorImpl(
            cause = cause
          )
    }
  }

  @Explanation("""This error occurs if there was an unexpected error within the interpreter""")
  @Resolution("Contact support.")
  object InternalError
      extends ErrorCode(
        id = "LEDGER_API_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    case class PackageSelfConsistency(
        err: LfError.Package.SelfConsistency
    )(implicit
        loggingContext: ErrorCodeLoggingContext
    ) extends LoggingTransactionErrorImpl(
          cause = err.message
        )

    case class PackageInternal(
        err: LfError.Package.Internal
    )(implicit
        loggingContext: ErrorCodeLoggingContext
    ) extends LoggingTransactionErrorImpl(
          cause = err.message
        )

    case class Preprocessing(
        err: LfError.Preprocessing.Internal
    )(implicit
        loggingContext: ErrorCodeLoggingContext
    ) extends LoggingTransactionErrorImpl(cause = err.message)

    case class Validation(reason: ReplayMismatch)(implicit
        loggingContext: ErrorCodeLoggingContext
    ) extends LoggingTransactionErrorImpl(
          cause = s"Observed un-expected replay mismatch: ${reason}"
        )

    case class Interpretation(
        where: String,
        message: String,
        detailMessage: Option[String],
    )(implicit
        loggingContext: ErrorCodeLoggingContext
    ) extends LoggingTransactionErrorImpl(
          cause = s"Daml-Engine interpretation failed with internal error: ${where} / ${message}"
        )

  }

  // The "NonHexOffset" error code is currently only used by canton, but should also be used by the ledger api services,
  // e.g. the ApiParticipantPruningService when it receives a non-hex-offset or by the other services such as the
  // transaction service that validate offsets using com.daml.ledger.api.validation.LedgerOffsetValidator.validate
  @Explanation("""The supplied offset could not be converted to a binary offset.""")
  @Resolution("Ensure the offset is specified as a hexadecimal string.")
  object NonHexOffset
      extends ErrorCode(
        id = "NON_HEXADECIMAL_OFFSET",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    case class Error(
        fieldName: String,
        offsetValue: String,
        message: String,
    )(implicit
        override val loggingContext: ErrorCodeLoggingContext
    ) extends BaseError.Impl(
          cause = s"Offset in ${fieldName} not specified in hexadecimal: ${offsetValue}: ${message}"
        )
  }
}
