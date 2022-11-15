// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error._
import com.daml.error.definitions.DamlErrorWithDefiniteAnswer
import com.daml.error.definitions.ErrorGroups.ParticipantErrorGroup.LedgerApiErrors
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.{VersionRange, language}

object CommandExecution {

  import LedgerApiErrors.CommandExecution.errorClass
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

    case class Reject(reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The participant failed to determine the max ledger time for this command: ${reason}"
        )
  }

  @Explanation("Command execution errors raised due to invalid packages.")
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
      ) extends DamlErrorWithDefiniteAnswer(
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
      ) extends DamlErrorWithDefiniteAnswer(
            cause = validationErrorCause
          )
    }
  }

  @Explanation(
    "Errors raised during command conversion to the internal data representation."
  )
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
      ) extends DamlErrorWithDefiniteAnswer(
            cause = err.message
          )
    }
  }

  @Explanation(
    "Errors raised during the command interpretation phase of the command submission evaluation."
  )
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
      ) extends DamlErrorWithDefiniteAnswer(
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
      ) extends DamlErrorWithDefiniteAnswer(
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
          err: LfInterpretationError.ContractNotActive,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          (ErrorResource.ContractId, err.coid.coid)
        )
      }

    }

    @Explanation("Errors raised in lookups during the command interpretation phase.")
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
            key: GlobalKey,
        )(implicit
            loggingContext: ContextualizedErrorLogger
        ) extends DamlErrorWithDefiniteAnswer(
              cause = cause
            ) {
          override def resources: Seq[(ErrorResource, String)] = Seq(
            (ErrorResource.ContractKey, key.toString())
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
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          )
    }
  }
}
