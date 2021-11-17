// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error._
import com.daml.error.definitions.ErrorGroups.ParticipantErrorGroup.LedgerApiErrorGroup
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Error.Validation.ReplayMismatch
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.lf.language.{LanguageVersion, LookupError, Reference}
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.{VersionRange, language}
import org.slf4j.event.Level

import java.time.{Duration, Instant}

object LedgerApiErrors extends LedgerApiErrorGroup {
  @Explanation(
    """This error occurs when a participant rejects a command due to excessive load.
        |Load can be caused by the following factors:
        |1. when commands are submitted to the participant through its Ledger API,
        |2. when the participant receives requests from other participants through a connected domain."""
  )
  @Resolution(
    """Wait a bit and retry, preferably with some backoff factor.
        |If possible, ask other participants to send fewer requests; the domain operator can enforce this by imposing a rate limit."""
  )
  object ParticipantBackpressure
      extends ErrorCode(
        id = "PARTICIPANT_BACKPRESSURE",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    override def logLevel: Level = Level.WARN

    case class Rejection(reason: String)(implicit errorLogger: ContextualizedErrorLogger)
        extends LoggingTransactionErrorImpl(cause = s"The participant is overloaded: $reason")
  }

  @Explanation(
    "This rejection is given when a request processing status is not known and a time-out is reached."
  )
  @Resolution(
    "Retry for transient problems. If non-transient contact the operator as the time-out limit might be too short."
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

  object CommandExecution extends ErrorGroup {
    @Explanation(
      """This error occurs if the participant fails to determine the max ledger time of the used
        |contracts. Most likely, this means that one of the contracts is not active anymore which can
        |happen under contention. It can also happen with contract keys.
        |"""
    )
    @Resolution("Retry the transaction submission.")
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

    object Package extends ErrorGroup() {
      @Explanation(
        """This error indicates that the uploaded DAR is based on an unsupported language version."""
      )
      @Resolution("Use a DAR compiled with a language version that this participant supports.")
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
        """This error occurs if a package referred to by a command fails validation. This should not happen as packages are validated when being uploaded."""
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

    object Preprocessing extends ErrorGroup {
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

    object Interpreter extends ErrorGroup {
      @Explanation("""This error occurs if a Daml transaction fails during interpretation.""")
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
        """This error occurs if a Daml transaction fails during interpretation due to an invalid argument."""
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
        @Explanation(
          """This error occurs if the Daml engine interpreter cannot resolve a contract key to an active contract. This
            |can be caused by either the contract key not being known to the participant, or not being known to
            |the submitting parties or the contract representing an already archived key."""
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
      }

      @Explanation("""This error occurs if a Daml transaction fails due to an authorization error.
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
  }

  @Explanation("This rejection is given when the requested service has already been closed.")
  @Resolution(
    "Retry re-submitting the request. If the error persists, contact the participant operator."
  )
  object ServiceNotRunning
      extends ErrorCode(
        id = "SERVICE_NOT_RUNNING",
        ErrorCategory.TransientServerFailure,
      ) {
    case class Reject(serviceName: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends LoggingTransactionErrorImpl(
          cause = s"$serviceName has been shut down."
        )

    case class ServiceReset(serviceName: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends LoggingTransactionErrorImpl(
          cause = s"$serviceName is currently being reset."
        )
  }

  object AuthorizationChecks extends ErrorGroup() {
    @Explanation(
      """This rejection is given if the submitted command does not contain a JWT token on a participant enforcing JWT authentication."""
    )
    @Resolution(
      "Ask your participant operator to provide you with an appropriate JWT token."
    )
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
      """This rejection is given if the supplied authorization token is not sufficient for the intended command.
        |The exact reason is logged on the participant, but not given to the user for security reasons."""
    )
    @Resolution(
      "Inspect your command and your token or ask your participant operator for an explanation why this command failed."
    )
    object PermissionDenied
        extends ErrorCode(id = "PERMISSION_DENIED", ErrorCategory.InsufficientPermission) {
      case class Reject(override val cause: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause =
              s"The provided authorization token is not sufficient to authorize the intended command: $cause"
          )
    }
  }

  object RequestValidation extends ErrorGroup {
    object NotFound extends ErrorGroup() {
      @Explanation(
        "This rejection is given when a read request tries to access a package which does not exist on the ledger."
      )
      @Resolution("Use a package id pertaining to a package existing on the ledger.")
      object Package
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

        case class InterpretationReject(
            packageId: PackageId,
            reference: Reference,
        )(implicit
            loggingContext: ContextualizedErrorLogger
        ) extends LoggingTransactionErrorImpl(
              cause = LookupError.MissingPackage.pretty(packageId, reference)
            )
      }

      @Explanation(
        "The transaction does not exist or the requesting set of parties are not authorized to fetch it."
      )
      @Resolution(
        "Check the transaction id and verify that the requested transaction is visible to the requesting parties."
      )
      object Transaction
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

      @Explanation(
        "The ledger configuration could not be retrieved. This could happen due to incomplete initialization of the participant or due to an internal system error."
      )
      @Resolution("Contact the participant operator.")
      object LedgerConfiguration
          extends ErrorCode(
            id = "LEDGER_CONFIGURATION_NOT_FOUND",
            ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
          ) {

        case class Reject()(implicit
            loggingContext: ContextualizedErrorLogger
        ) extends LoggingTransactionErrorImpl(
              cause = "The ledger configuration could not be retrieved."
            )
      }
    }

    @Explanation("This rejection is given when a read request tries to access pruned data.")
    @Resolution("Use an offset that is after the pruning offset.")
    object ParticipantPrunedDataAccessed
        extends ErrorCode(
          id = "PARTICIPANT_PRUNED_DATA_ACCESSED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
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
    object OffsetAfterLedgerEnd
        extends ErrorCode(
          id = "OFFSET_AFTER_LEDGER_END",
          ErrorCategory.InvalidGivenCurrentSystemStateSeekAfterEnd,
        ) {
      case class Reject(offsetType: String, requestedOffset: String, ledgerEnd: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = s"$offsetType offset ($requestedOffset) is after ledger end ($ledgerEnd)"
          )
    }

    @Explanation(
      "This rejection is given when a read request uses an offset invalid in the requests' context."
    )
    @Resolution("Inspect the error message and use a valid offset.")
    object OffsetOutOfRange
        extends ErrorCode(
          id = "OFFSET_OUT_OF_RANGE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Reject(message: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(cause = message)
    }

    @Explanation(
      """Every ledger API command contains a ledger-id which is verified against the running ledger.
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
      """This error is emitted when a mandatory field is not set in a submitted ledger API command."""
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
      """This error is emitted when a submitted ledger API command contains an invalid argument."""
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
      """This error is emitted when a submitted ledger API command contains a field value that cannot be understood."""
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
      "Inspect the error message, adjust the value of the deduplication period or ask the participant operator to increase the maximum deduplication period."
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
            cause =
              s"Offset in ${fieldName} not specified in hexadecimal: ${offsetValue}: ${message}"
          )
    }
  }

  @Explanation("""This error occurs if there was an unexpected error in the Ledger API.""")
  @Resolution("Contact support.")
  object InternalError
      extends ErrorCode(
        id = "LEDGER_API_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    case class CommandTrackerInternalError(
        message: String,
        override val throwableO: Option[Throwable] = None,
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

    case class VersionService(message: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends LoggingTransactionErrorImpl(cause = message)
  }

  object AdminServices {
    @Explanation("This rejection is given when a new configuration is rejected.")
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

    @Explanation("This rejection is given when a package upload is rejected.")
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
  }

  object ConsistencyErrors extends ErrorGroup {
    @Explanation("A command with the given command id has already been successfully processed.")
    @Resolution(
      """The correct resolution depends on the use case. If the error received pertains to a submission retried due to a timeout,
        |do nothing, as the previous command has already been accepted.
        |If the intent is to submit a new command, re-submit using a distinct command id. 
        |"""
    )
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

    @Explanation(
      """This error occurs if the Daml engine can not find a referenced contract. This
        |can be caused by either the contract not being known to the participant, or not being known to
        |the submitting parties or already being archived."""
    )
    @Resolution("This error type occurs if there is contention on a contract.")
    object ContractNotFound
        extends ErrorCode(
          id = "CONTRACT_NOT_FOUND",
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
      case class RejectWithContractKeyArg(
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

      case class Reject(override val cause: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(cause = cause)
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

  object WriteServiceRejections extends ErrorGroup {
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
    @Resolution(
      "Inspect the error message and retry after after correcting the underlying issue."
    )
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
      case class RejectWithSubmitterAndParticipantId(
          details: String,
          submitter: String,
          participantId: String,
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(cause = s"Inconsistent: $details")

      case class Reject(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(cause = s"Inconsistent: $details")
    }
  }
}
