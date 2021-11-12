// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import java.time.Duration
import com.daml.error._
import com.daml.error.definitions.ErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LedgerApiErrorGroup
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Error.Validation.ReplayMismatch
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.lf.language.{LanguageVersion, LookupError, Reference}
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.{VersionRange, language}

import java.time.Instant

object LedgerApiErrors extends LedgerApiErrorGroup {

  @Explanation("This rejection is given when the requested service has already been closed.")
  @Resolution("Contact the participant operator.")
  object ServiceNotRunning
      extends ErrorCode(
        id = "SERVICE_NOT_RUNNING",
        // TODO error codes: Re-check this error category
        ErrorCategory.TransientServerFailure,
      ) {
    case class Reject()(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends LoggingTransactionErrorImpl(
          cause = "Service has been shut down."
        )
  }

  object WriteErrors extends ErrorGroup() {
    @Explanation("This rejection is given when a configuration entry write was rejected.")
    @Resolution("Fetch newest configuration and/or retry.")
    object ConfigurationEntryRejected
        extends ErrorCode(
          id = "CONFIGURATION_ENTRY_REJECTED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Reject(message: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = message
          )
    }

    @Explanation("This rejection is given when a package upload was rejected.")
    @Resolution("Refer to the detailed message of the received error.")
    object PackageUploadRejected
        extends ErrorCode(
          id = "PACKAGE_UPLOAD_REJECTED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Reject(message: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = message
          )
    }

    @Explanation(
      "This rejection is given when a request might not been processed and a time-out was reached."
    )
    @Resolution(
      "Retry for transient problems. If non-transient contact the operator as the time-out limit might be to short."
    )
    object RequestTimeOut
        extends ErrorCode(
          id = "REQUEST_TIME_OUT",
          ErrorCategory.DeadlineExceededRequestStateUnknown,
        ) {
      case class Reject(message: String, override val definiteAnswer: Boolean)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = message
          )
    }
  }

  object ReadErrors extends ErrorGroup() {

    @Explanation("This rejection is given when a package id is malformed.")
    @Resolution("Make sure the package id provided in the request has correct form.")
    // TODO error codes: Consider using `LedgerApiErrors.CommandValidation.InvalidArgument`
    object MalformedPackageId
        extends ErrorCode(
          id = "MALFORMED_PACKAGE_ID",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      case class Reject(message: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = message
          )
    }

    @Explanation(
      "This rejection is given when a read request tries to access a package which does not exist on the ledger."
    )
    @Resolution("Use a package id pertaining to a package existing on the ledger.")
    // TODO error codes: Possible duplicate of `LedgerApiErrors.Package.MissingPackage`
    object PackageNotFound
        extends ErrorCode(
          id = "PACKAGE_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      case class Reject(packageId: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = "Could not find package."
          ) {

        override def resources: Seq[(ErrorResource, String)] = {
          super.resources :+ ((ErrorResource.DalfPackage, packageId))
        }
      }
    }

    @Explanation("This rejection is given when a read request tries to access pruned data.")
    @Resolution("Use an offset that is after the pruning offset.")
    object ParticipantPrunedDataAccessed
        extends ErrorCode(
          id = "PARTICIPANT_PRUNED_DATA_ACCESSED",
          // TODO error codes: Rename error category to cover this scenario
          //                   where the data accessed is before the allowed pruning begin
          ErrorCategory.InvalidGivenCurrentSystemStateSeekAfterEnd,
        ) {
      case class Reject(message: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = message
          )
    }

    @Explanation(
      "This rejection is given when a read request uses an offset beyond the current ledger end."
    )
    @Resolution("Use an offset that is before the ledger end.")
    object RequestedOffsetOutOfRange
        extends ErrorCode(
          id = "REQUESTED_OFFSET_OUT_OF_RANGE",
          ErrorCategory.InvalidGivenCurrentSystemStateSeekAfterEnd,
        ) {
      case class Reject(message: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = message
          )
    }

    @Explanation(
      "The transaction does not exist or the requesting set of parties are not authorized to fetch it."
    )
    @Resolution(
      "Check the transaction id and verify that the requested transaction is visible to the requesting parties."
    )
    object TransactionNotFound
        extends ErrorCode(
          id = "TRANSACTION_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {

      case class Reject(transactionId: String)(implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(cause = "Transaction not found, or not visible.") {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          (ErrorResource.TransactionId, transactionId)
        )
      }
    }
  }

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
      case class MissingJwtToken()(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = "The command is missing a JWT token"
          )
    }

    @Explanation("An internal system authorization error occurred.")
    @Resolution("Contact the participant operator.")
    object InternalAuthorizationError
        extends ErrorCode(
          id = "INTERNAL_AUTHORIZATION_ERROR",
          ErrorCategory.SystemInternalAssumptionViolated,
        ) {
      case class Reject(message: String, throwable: Throwable)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = message,
            throwableO = Some(throwable),
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
      case class Reject(override val cause: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause =
              s"The provided JWT token is not sufficient to authorize the intended command: $cause"
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
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = cause,
            definiteAnswer = true,
          )
    }

    @Explanation(
      """This error is emitted, when a submitted ledger Api command could not be successfully deserialized due to mandatory fields not being set."""
    )
    @Resolution("Inspect the reason given and correct your application.")
    object MissingField
        extends ErrorCode(id = "MISSING_FIELD", ErrorCategory.InvalidIndependentOfSystemState) {
      case class Reject(missingField: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = s"The submitted command is missing a mandatory field: $missingField"
          )
    }

    @Explanation(
      """This error is emitted, when a submitted ledger Api command contained an invalid argument."""
    )
    @Resolution("Inspect the reason given and correct your application.")
    object InvalidArgument
        extends ErrorCode(id = "INVALID_ARGUMENT", ErrorCategory.InvalidIndependentOfSystemState) {
      case class Reject(_reason: String)(implicit
          loggingContext: ContextualizedErrorLogger
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
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = s"The submitted command has a field with invalid value: ${_reason}"
          )
    }

    @Explanation(
      "This error is emitted when a submitted ledger API command specifies an invalid deduplication period."
    )
    @Resolution(
      "Inspect the error message, adjust the value of the deduplication period or ask the participant operator to increase the maximum."
    )
    object InvalidDeduplicationPeriodField
        extends ErrorCode(
          id = "INVALID_DEDUPLICATION_PERIOD",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Reject(_reason: String, _maxDeduplicationDuration: Duration)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = s"The submitted command had an invalid deduplication period: ${_reason}"
          ) {
        override def context: Map[String, String] =
          super.context + ("max_deduplication_duration" -> _maxDeduplicationDuration.toString)
      }
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

      case class Reject(override val definiteAnswer: Boolean = false)(implicit
          loggingContext: ContextualizedErrorLogger
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
          loggingContext: ContextualizedErrorLogger
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
          val loggingContext: ContextualizedErrorLogger
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
          loggingContext: ContextualizedErrorLogger
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
          loggingContext: ContextualizedErrorLogger
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
          loggingContext: ContextualizedErrorLogger
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
          loggingContext: ContextualizedErrorLogger
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
          loggingContext: ContextualizedErrorLogger
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
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = cause
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          (ErrorResource.ContractId, _err.coid.coid)
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
            loggingContext: ContextualizedErrorLogger
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
            loggingContext: ContextualizedErrorLogger
        ) extends LoggingTransactionErrorImpl(
              cause = cause
            ) {
          override def resources: Seq[(ErrorResource, String)] = Seq(
            (ErrorResource.ContractKey, _key.toString())
          )
        }

      }

      @Explanation(
        "The ledger configuration could not be retrieved. This could happen due to incomplete initialization of the participant or due to an internal system error."
      )
      @Resolution("Contact the participant operator.")
      object LedgerConfigurationNotFound
          extends ErrorCode(
            id = "LEDGER_CONFIGURATION_NOT_FOUND",
            ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
          ) {

        case class Reject()(implicit
            loggingContext: ContextualizedErrorLogger
        ) extends LoggingTransactionErrorImpl(
              cause = "The ledger configuration is not available."
            )
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
          loggingContext: ContextualizedErrorLogger
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

    // TODO error codes: This is an internal error not related to the interpreter
    case class CommandTrackerInternalError(
        message: String
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends LoggingTransactionErrorImpl(cause = message)

    case class PackageSelfConsistency(
        err: LfError.Package.SelfConsistency
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends LoggingTransactionErrorImpl(
          cause = err.message
        )

    case class PackageInternal(
        err: LfError.Package.Internal
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends LoggingTransactionErrorImpl(
          cause = err.message
        )

    case class Preprocessing(
        err: LfError.Preprocessing.Internal
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends LoggingTransactionErrorImpl(cause = err.message)

    case class Validation(reason: ReplayMismatch)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends LoggingTransactionErrorImpl(
          cause = s"Observed un-expected replay mismatch: ${reason}"
        )

    case class Interpretation(
        where: String,
        message: String,
        detailMessage: Option[String],
    )(implicit
        loggingContext: ContextualizedErrorLogger
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
        override val loggingContext: ContextualizedErrorLogger
    ) extends BaseError.Impl(
          cause = s"Offset in ${fieldName} not specified in hexadecimal: ${offsetValue}: ${message}"
        )
  }

  object VersionServiceError extends ErrorGroup {
    @Explanation("This error occurs if there was an unexpected error within the version service.")
    @Resolution("Contact support.")
    object InternalError
        extends ErrorCode(
          id = "VERSION_SERVICE_INTERNAL_ERROR",
          ErrorCategory.SystemInternalAssumptionViolated,
        ) {

      case class Reject(message: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(cause = message)
    }
  }

  object CommandRejections extends ErrorGroup {
    @Explanation("An input contract has been archived by a concurrent transaction submission.")
    @Resolution(
      "The correct resolution depends on the business flow, for example it may be possible to " +
        "proceed without the archived contract as an input, or a different contract could be used."
    )
    object InconsistentContracts
        extends ErrorCode(
          id = "INCONSISTENT_CONTRACTS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Reject(override val cause: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(cause = cause)
    }

    @Explanation("The submitting party has not been allocated.")
    @Resolution(
      "Check that the party identifier is correct, allocate the submitting party, " +
        "request its allocation or wait for it to be allocated before retrying the transaction submission."
    )
    object SubmittingPartyNotKnownOnLedger
        extends ErrorCode(
          id = "SUBMITTING_PARTY_NOT_KNOWN_ON_LEDGER",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing, // It may become known at a later time
        ) {
      case class Reject(
          submitter_party: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(
            cause = s"Party not known on ledger: Submitting party '$submitter_party' not known"
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          ErrorResource.Party -> submitter_party
        )
      }
    }

    @Explanation("One or more informee parties have not been allocated.")
    @Resolution(
      "Check that all the informee party identifiers are correct, allocate all the informee parties, " +
        "request their allocation or wait for them to be allocated before retrying the transaction submission."
    )
    object PartyNotKnownOnLedger
        extends ErrorCode(
          id = "PARTY_NOT_KNOWN_ON_LEDGER",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      case class Reject(parties: Set[String])(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(cause = s"Parties not known on ledger: ${parties
            .mkString("[", ",", "]")}") {
        override def resources: Seq[(ErrorResource, String)] =
          parties.map((ErrorResource.Party, _)).toSeq
      }

      @deprecated
      case class RejectDeprecated(
          description: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(
            cause = s"Party not known on ledger: $description"
          )
    }

    @Explanation("At least one input has been altered by a concurrent transaction submission.")
    @Resolution(
      "The correct resolution depends on the business flow, for example it may be possible to proceed " +
        "without an archived contract as an input, or the transaction submission may be retried " +
        "to load the up-to-date value of a contract key."
    )
    object Inconsistent
        extends ErrorCode(
          id = "INCONSISTENT",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Reject(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(
            cause = s"Inconsistent: $details"
          )
    }

    @Explanation("""This error occurs if the Daml engine interpreter can not find a referenced contract. This
                   |can be caused by either the contract not being known to the participant, or not being known to
                   |the submitting parties or already being archived.""")
    @Resolution("This error type occurs if there is contention on a contract.")
    object ContractsNotFound
        extends ErrorCode(
          id = "CONTRACTS_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {

      case class MultipleContractsNotFound(notFoundContractIds: Set[String])(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = s"Unknown contracts: ${notFoundContractIds.mkString("[", ", ", "]")}"
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          (ErrorResource.ContractId, notFoundContractIds.mkString("[", ", ", "]"))
        )
      }
    }

    @Explanation(
      "An input contract key was re-assigned to a different contract by a concurrent transaction submission."
    )
    @Resolution("Retry the transaction submission.")
    object InconsistentContractKey
        extends ErrorCode(
          id = "INCONSISTENT_CONTRACT_KEY",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Reject(reason: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(cause = reason)
    }

    @Explanation(
      """This error signals that within the transaction we got to a point where two contracts with the same key were active."""
    )
    @Resolution("This error indicates an application error.")
    object DuplicateContractKey
        extends ErrorCode(
          id = "DUPLICATE_CONTRACT_KEY",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
        ) {
      case class InterpretationReject(
          override val cause: String,
          _key: GlobalKey,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = cause
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          // TODO error codes: Reconsider the transport format for the contract key.
          //                   If the key is big, it can force chunking other resources.
          (ErrorResource.ContractKey, _key.toString())
        )
      }

      case class LedgerReject(override val cause: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(cause = cause)
    }

    @Explanation("An invalid transaction submission was not detected by the participant.")
    @Resolution("Contact support.")
    @deprecated("Corresponds to transaction submission rejections that are not produced anymore.")
    @DeprecatedDocs(
      "Corresponds to transaction submission rejections that are not produced anymore."
    )
    object Disputed
        extends ErrorCode(
          id = "DISPUTED",
          ErrorCategory.SystemInternalAssumptionViolated, // It should have been caught by the participant
        ) {
      case class Reject(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(
            cause = s"Disputed: $details"
          )
    }

    @Explanation(
      "The Participant node did not have sufficient resource quota to submit the transaction."
    )
    @Resolution("Inspect the error message and retry after after correcting the underlying issue.")
    @deprecated("Corresponds to transaction submission rejections that are not produced anymore.")
    @DeprecatedDocs(
      "Corresponds to transaction submission rejections that are not produced anymore."
    )
    object OutOfQuota
        extends ErrorCode(id = "OUT_OF_QUOTA", ErrorCategory.ContentionOnSharedResources) {
      case class Reject(reason: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(cause = reason)
    }

    @Explanation("A submitting party is not authorized to act through the participant.")
    @Resolution("Contact the participant operator or re-submit with an authorized party.")
    object SubmitterCannotActViaParticipant
        extends ErrorCode(
          id = "SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT",
          ErrorCategory.InsufficientPermission,
        ) {
      case class Reject(
          details: String,
          submitter: String = "N/A",
          participantId: String = "N/A",
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(cause = s"Inconsistent: $details")
    }

    @Explanation(
      "The ledger time of the submission violated some constraint on the ledger time."
    )
    @Resolution("Retry the transaction submission.")
    object InvalidLedgerTime
        extends ErrorCode(
          id = "INVALID_LEDGER_TIME",
          ErrorCategory.InvalidGivenCurrentSystemStateOther, // It may succeed at a later time
        ) {
      case class RejectEnriched(
          details: String,
          ledger_time: Instant,
          ledger_time_lower_bound: Instant,
          ledger_time_upper_bound: Instant,
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(cause = s"Invalid ledger time: $details")

      case class RejectSimple(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(cause = s"Invalid ledger time: $details")
    }
  }
}
