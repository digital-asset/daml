// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error.groups

import com.digitalasset.base.error.{
  ContextualizedErrorLogger,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCategoryRetry,
  ErrorCode,
  ErrorGroup,
  ErrorResource,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.ledger.error.ParticipantErrorGroup.LedgerApiErrorGroup.CommandExecutionErrorGroup
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId}
import com.digitalasset.daml.lf.engine.Error as LfError
import com.digitalasset.daml.lf.interpretation.Error as LfInterpretationError
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, Reference}
import com.digitalasset.daml.lf.transaction.{GlobalKey, TransactionVersion}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.{Value, ValueCoder}
import com.digitalasset.daml.lf.{VersionRange, language}
import com.google.common.io.BaseEncoding
import org.slf4j.event.Level

import scala.concurrent.duration.DurationInt

@Explanation(
  "Errors raised during the command execution phase of the command submission evaluation."
)
object CommandExecutionErrors extends CommandExecutionErrorGroup {
  def encodeValue(v: Value): Either[ValueCoder.EncodeError, String] =
    ValueCoder
      .encodeValue(valueVersion = TransactionVersion.VDev, v0 = v)
      .map(bs => BaseEncoding.base64().encode(bs.toByteArray))

  def withEncodedValue(
      v: Value
  )(
      f: String => Seq[(ErrorResource, String)]
  )(implicit loggingContext: ContextualizedErrorLogger): Seq[(ErrorResource, String)] =
    encodeValue(v).fold(
      { case ValueCoder.EncodeError(msg) =>
        loggingContext.error(msg)
        Seq.empty
      },
      f,
    )

  def encodeParties(parties: Set[Ref.Party]): Seq[(ErrorResource, String)] =
    Seq((ErrorResource.Parties, parties.mkString(",")))

  @Explanation(
    """This error occurs if the participant fails to execute a transaction via the interactive submission service.
      |"""
  )
  @Resolution("Inspect error details and report the error.")
  object InteractiveSubmissionExecuteError
      extends ErrorCode(
        id = "FAILED_TO_EXECUTE_TRANSACTION",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {

    final case class Reject(reason: String, throwable: Option[Throwable] = None)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"The participant failed to execute the transaction: $reason",
          throwableO = throwable,
        )
  }

  @Explanation(
    """This error occurs if the participant fails to prepare a transaction via the interactive submission service.
      |"""
  )
  @Resolution("Inspect error details and report the error.")
  object InteractiveSubmissionPreparationError
      extends ErrorCode(
        id = "FAILED_TO_PREPARE_TRANSACTION",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {

    final case class Reject(reason: String, throwable: Option[Throwable] = None)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"The participant failed to prepare the transaction: $reason",
          throwableO = throwable,
        )
  }

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

    final case class Reject(reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The participant failed to determine the max ledger time for this command: $reason"
        )
  }

  @Explanation(
    """This error occurs when the interpretation of a command exceeded the time limit, defined
      |as the maximum time that can be assigned by the ledger when it starts processing the command.
      |It corresponds to the time assigned upon submission by the participant (the ledger time) + a tolerance
      |defined by the `ledgerTimeToRecordTimeTolerance` ledger configuration parameter.
      |Reasons for exceeding this limit can vary: the participant may be under high load, the command interpretation
      |may be very complex, or even run into an infinite loop due to a mistake in the Daml code.
      |"""
  )
  @Resolution(
    """Due to the halting problem, we cannot determine whether the interpretation will eventually complete.
      |As a developer: inspect your code for possible non-terminating loops or consider reducing its complexity.
      |As an operator: check and possibly update the resources allocated to the system, as well as the
      |time-related configuration parameters (see "Time on Daml Ledgers" in the "Daml Ledger Model Concepts" doc section
      |and the `set_ledger_time_record_time_tolerance` console command).
      |"""
  )
  object TimeExceeded //
      extends ErrorCode(
        id = "INTERPRETATION_TIME_EXCEEDED",
        ErrorCategory.ContentionOnSharedResources,
      ) {

    override def logLevel: Level = Level.WARN

    final case class Reject(reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = reason) {
      override def retryable: Option[ErrorCategoryRetry] = Some(
        // As we cannot tell whether the command timed out due to running into an infinite loop,
        // because it's too complex, or because the system resources are under heavy load, we need to give
        // the application the opportunity to retry. It should not retry "too quickly" though, to avoid entering
        // a fast cycle of retry-abort.
        // 60 seconds is in the ballpark of the default ledger-time-to-record-time tolerance, so is a reasonable
        // amount of time to wait before retrying.
        ErrorCategoryRetry(duration = 60.seconds)
      )
    }
  }

  @Explanation(
    """This error occurs if some of the disclosed contracts attached to the command submission that were also used in command interpretation have specified mismatching synchronizer ids.
      |This can happen if the synchronizer ids of the disclosed contracts are out of sync OR if the originating contracts are assigned to different synchronizers."""
  )
  @Resolution(
    "Retry the submission with an up-to-date set of attached disclosed contracts or re-create a command submission that only uses disclosed contracts residing on the same synchronizer."
  )
  object DisclosedContractsSynchronizerIdMismatch
      extends ErrorCode(
        id = "DISCLOSED_CONTRACTS_SYNCHRONIZER_ID_MISMATCH",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Reject(mismatchingContractIdToSynchronizerIds: Map[ContractId, String])(
        implicit loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"Some disclosed contracts that were used during command interpretation have mismatching synchronizer ids: $mismatchingContractIdToSynchronizerIds"
        )
  }

  @Explanation(
    """This error occurs when the synchronizer id provided in the command submission mismatches the synchronizer id specified in one of the disclosed contracts used in command interpretation."""
  )
  @Resolution(
    "Retry the submission with all disclosed contracts residing on the target submission synchronizer."
  )
  object PrescribedSynchronizerIdMismatch
      extends ErrorCode(
        id = "PRESCRIBED_SYNCHRONIZER_ID_MISMATCH",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Reject(
        usedDisclosedContractsSpecifyingASynchronizerId: Set[ContractId],
        disclosedContractsSynchronizerId: String,
        prescribedSynchronizerId: String,
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The target synchronizer=$prescribedSynchronizerId specified in the command submission mismatches the synchronizer id=$disclosedContractsSynchronizerId of some attached disclosed contracts that have been used in the submission (used-disclosed-contract-ids=$usedDisclosedContractsSpecifyingASynchronizerId)"
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

      final case class Error(
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
          ErrorCategory.SecurityAlert,
        ) {
      final case class Reject(validationErrorCause: String)(implicit
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
      final case class Reject(
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

      final case class Error(override val cause: String)(implicit
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

      final case class Error(override val cause: String)(implicit
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

      object Reject {
        def apply(cause: String, err: LfInterpretationError.ContractNotActive)(implicit
            loggingContext: ContextualizedErrorLogger
        ): Reject = Reject(
          cause,
          err.coid,
          Some(err.templateId.toString()),
        )
      }

      final case class Reject(
          override val cause: String,
          coid: ContractId,
          templateIdO: Option[String],
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          templateIdO.map(templateId => (ErrorResource.TemplateId, templateId)),
          Some((ErrorResource.ContractId, coid.coid)),
        ).flatten
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

        final case class Reject(
            override val cause: String,
            key: GlobalKey,
        )(implicit
            loggingContext: ContextualizedErrorLogger
        ) extends DamlErrorWithDefiniteAnswer(
              cause = cause
            ) {
          override def resources: Seq[(ErrorResource, String)] =
            withEncodedValue(key.key) { encodedKey =>
              Seq(
                (ErrorResource.TemplateId, key.templateId.toString),
                (ErrorResource.ContractKey, encodedKey),
                (ErrorResource.PackageName, key.packageName),
              )
            }
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

      final case class Reject(override val cause: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          )
    }

    @Explanation(
      """This error occurs if a user attempts to provide a key hash for a disclosed contract which we have already cached to be different."""
    )
    @Resolution(
      "Ensure the contract ID and contract payload you have provided in your disclosed contract is correct."
    )
    object DisclosedContractKeyHashingError
        extends ErrorCode(
          id = "DISCLOSED_CONTRACT_KEY_HASHING_ERROR",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.DisclosedContractKeyHashingError,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {

        override def resources: Seq[(ErrorResource, String)] =
          withEncodedValue(err.key.key) { encodedKey =>
            Seq(
              (ErrorResource.TemplateId, err.key.templateId.toString),
              (ErrorResource.ContractId, err.coid.coid),
              (ErrorResource.ContractKey, encodedKey),
              (ErrorResource.ContractKeyHash, err.declaredHash.toString),
              (ErrorResource.PackageName, err.key.packageName),
            )
          }
      }
    }

    private def getTypeIdentifier(t: Ast.Type): Option[Identifier] =
      t match {
        case Ast.TTyCon(ty) => Some(ty)
        case _ => None
      }

    @Explanation(
      """This error occurs when a user throws an error and does not catch it with try-catch."""
    )
    @Resolution(
      "Either your error handling in a choice body is insufficient, or you are using a contract incorrectly."
    )
    object UnhandledException
        extends ErrorCode(
          id = "UNHANDLED_EXCEPTION",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.UnhandledException,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {
        override def resources: Seq[(ErrorResource, String)] =
          withEncodedValue(err.value) { encodedValue =>
            getTypeIdentifier(err.exceptionType)
              .map(ty =>
                Seq(
                  (ErrorResource.ExceptionType, ty.toString),
                  (ErrorResource.ExceptionValue, encodedValue),
                )
              )
              .getOrElse(Nil)
          }
      }
    }

    @Explanation(
      """This error occurs when a user calls abort or error on an LF version before native exceptions were introduced."""
    )
    @Resolution(
      "Either remove the call to abort, error or perhaps assert, or ensure you are exercising your contract choice as the author expects."
    )
    object InterpretationUserError
        extends ErrorCode(
          id = "INTERPRETATION_USER_ERROR",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.UserError,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {
        override def resources: Seq[(ErrorResource, String)] =
          Seq(
            (ErrorResource.ExceptionText, err.message)
          )
      }
    }

    @Explanation(
      """This error occurs when a contract's pre-condition (the ensure clause) is violated on contract creation."""
    )
    @Resolution(
      "Ensure the contract argument you are passing into your create doesn't violate the conditions of the contract."
    )
    object TemplatePreconditionViolated
        extends ErrorCode(
          id = "TEMPLATE_PRECONDITION_VIOLATED",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Reject(
          override val cause: String
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          )
    }

    @Explanation(
      """This error occurs when you try to create a contract that has a key, but with empty maintainers."""
    )
    @Resolution(
      "Check the definition of the contract key's maintainers, and ensure this list won't be empty given your creation arguments."
    )
    object CreateEmptyContractKeyMaintainers
        extends ErrorCode(
          id = "CREATE_EMPTY_CONTRACT_KEY_MAINTAINERS",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.CreateEmptyContractKeyMaintainers,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {

        override def resources: Seq[(ErrorResource, String)] =
          withEncodedValue(err.arg) { encodedArg =>
            Seq(
              (ErrorResource.TemplateId, err.templateId.toString),
              (ErrorResource.ContractArg, encodedArg),
            )
          }
      }
    }

    @Explanation(
      """This error occurs when you try to fetch a contract by key, but that key would have empty maintainers."""
    )
    @Resolution(
      "Check the definition of the contract key's maintainers, and ensure this list won't be empty given the contract key you are fetching."
    )
    object FetchEmptyContractKeyMaintainers
        extends ErrorCode(
          id = "FETCH_EMPTY_CONTRACT_KEY_MAINTAINERS",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.FetchEmptyContractKeyMaintainers,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {

        override def resources: Seq[(ErrorResource, String)] =
          withEncodedValue(err.key) { encodedKey =>
            Seq(
              (ErrorResource.TemplateId, err.templateId.toString),
              (ErrorResource.ContractKey, encodedKey),
              (ErrorResource.PackageName, err.packageName),
            )
          }
      }
    }

    @Explanation(
      """This error occurs when you try to fetch/use a contract in some way with a contract ID that doesn't match the template type on the ledger."""
    )
    @Resolution(
      "Ensure the contract IDs you are using are of the type we expect on the ledger. Avoid unsafely coercing contract IDs."
    )
    object WronglyTypedContract
        extends ErrorCode(
          id = "WRONGLY_TYPED_CONTRACT",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.WronglyTypedContract,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {

        override def resources: Seq[(ErrorResource, String)] =
          Seq(
            (ErrorResource.ContractId, err.coid.coid),
            (ErrorResource.TemplateId, err.expected.toString),
            (ErrorResource.TemplateId, err.actual.toString),
          )
      }
    }

    @Explanation(
      """This error occurs when you try to coerce/use a contract via an interface that it does not implement."""
    )
    @Resolution(
      "Ensure the contract you are calling does implement the interface you are using to do so. Avoid writing LF/low-level interface implementation classes manually."
    )
    object ContractDoesNotImplementInterface
        extends ErrorCode(
          id = "CONTRACT_DOES_NOT_IMPLEMENT_INTERFACE",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.ContractDoesNotImplementInterface,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {

        override def resources: Seq[(ErrorResource, String)] =
          Seq(
            (ErrorResource.ContractId, err.coid.coid),
            (ErrorResource.TemplateId, err.templateId.toString),
            (ErrorResource.InterfaceId, err.interfaceId.toString),
          )
      }
    }

    @Explanation(
      """This error occurs when you try to create/use a contract that does not implement the requiring interfaces of some other interface that it does implement."""
    )
    @Resolution(
      "Ensure you implement all required interfaces correctly, and avoid writing LF/low-level interface implementation classes manually."
    )
    object ContractDoesNotImplementRequiringInterface
        extends ErrorCode(
          id = "CONTRACT_DOES_NOT_IMPLEMENT_REQUIRING_INTERFACE",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.ContractDoesNotImplementRequiringInterface,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {

        override def resources: Seq[(ErrorResource, String)] =
          Seq(
            (ErrorResource.ContractId, err.coid.coid),
            (ErrorResource.TemplateId, err.templateId.toString),
            (ErrorResource.InterfaceId, err.requiredInterfaceId.toString),
            (ErrorResource.InterfaceId, err.requiringInterfaceId.toString),
          )
      }
    }

    @Explanation(
      """This error occurs when you attempt to compare two values of different types using the built-in comparison types."""
    )
    @Resolution(
      "Avoid using the low level comparison build, and instead use the Eq class."
    )
    object NonComparableValues
        extends ErrorCode(
          id = "NON_COMPARABLE_VALUES",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Reject(
          override val cause: String
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          )
    }

    @Explanation(
      """This error occurs when a contract key contains a contract ID, which is illegal for hashing reasons."""
    )
    @Resolution(
      "Ensure your contracts key field cannot contain a contract ID."
    )
    object ContractIdInContractKey
        extends ErrorCode(
          id = "CONTRACT_ID_IN_CONTRACT_KEY",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Reject(
          override val cause: String
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          )
    }

    @Explanation(
      """This error occurs when you attempt to compare a global and local contract ID of the same discriminator."""
    )
    @Resolution(
      "Avoid constructing contract IDs manually."
    )
    object ContractIdComparability
        extends ErrorCode(
          id = "CONTRACT_ID_COMPARABILITY",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.ContractIdComparability,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {

        override def resources: Seq[(ErrorResource, String)] =
          Seq(
            (ErrorResource.ContractId, err.globalCid.coid)
          )
      }
    }

    @Explanation("This error occurs when you nest values too deeply.")
    @Resolution("Restructure your code and reduce value nesting.")
    object ValueNesting
        extends ErrorCode(id = "VALUE_NESTING", ErrorCategory.InvalidIndependentOfSystemState) {

      final case class Reject(override val cause: String, err: LfInterpretationError.ValueNesting)(
          implicit loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = cause) {}
    }

    @Explanation("This error is thrown by use of `failWithStatus` in daml code. The Daml code determines the canton error category, and thus the grpc status code.")
    @Resolution("Either your choice body has a bug, or you are using a contract incorrectly.")
    object FailureStatus
        extends ErrorCode(id = "DAML_FAILURE", ErrorCategory.OverrideDocStringErrorCategory("<determined by daml code>")) {

      // Override the code to change the category, otherwise inherit frokm FailureStatus
      private def mkErrorCode(err: LfInterpretationError.FailureStatus): ErrorCode =
        new ErrorCode(id = FailureStatus.id, ErrorCategory.fromInt(err.failureCategory).getOrElse(FailureStatus.category))(FailureStatus.parent) {}

      // Building conveyance string from OverrideDocStringErrorCategory will fail, and would be wrong
      override def errorConveyanceDocString: Option[String] = Some("Conveyance is determined by the category, which is selected in daml code")

      final case class Reject(override val cause: String, err: LfInterpretationError.FailureStatus)(
          implicit loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = cause)(code = mkErrorCode(err), loggingContext = loggingContext) {
        override def context: Map[String, String] =
          // ++ on maps takes last key, we don't want users to override `error_id`, so we add this last
          // SerializableErrorCodeComponents also puts `context` first, so fields added by canton cannot be overwritten
          super.context ++ err.metadata ++ List(("error_id", err.errorId))
      }
    }

    @Explanation("Errors that occur when trying to upgrade a contract")
    object UpgradeError extends ErrorGroup {
      @Explanation("Validation fails when trying to upgrade the contract")
      @Resolution(
        "Verify that neither the signatories, nor the observers, nor the contract key, nor the key's maintainers have changed"
      )
      object ValidationFailed
          extends ErrorCode(
            id = "INTERPRETATION_UPGRADE_ERROR_VALIDATION_FAILED",
            ErrorCategory.InvalidGivenCurrentSystemStateOther,
          ) {
        final case class Reject(
            override val cause: String,
            err: LfInterpretationError.Upgrade.ValidationFailed,
        )(implicit
            loggingContext: ContextualizedErrorLogger
        ) extends DamlErrorWithDefiniteAnswer(
              cause = cause
            ) {

          override def resources: Seq[(ErrorResource, String)] = {
            val optKeyResources = err.keyOpt.fold(Seq.empty[(ErrorResource, String)])(key =>
              withEncodedValue(key.globalKey.key) { encodedKey =>
                Seq(
                  (ErrorResource.ContractKey, encodedKey),
                  (ErrorResource.PackageName, key.globalKey.packageName),
                ) ++ encodeParties(key.maintainers)
              }
            )

            Seq(
              (ErrorResource.ContractId, err.coid.coid),
              (ErrorResource.TemplateId, err.srcTemplateId.toString),
              (ErrorResource.TemplateId, err.dstTemplateId.toString),
            ) ++ encodeParties(err.signatories) ++ encodeParties(err.observers) ++ optKeyResources
          }
        }
      }

      @Explanation(
        "An optional contract field with a value of Some may not be dropped during downgrading"
      )
      @Resolution(
        "There is data that is newer than the implementation using it, and thus is not compatible. Ensure new data (i.e. those with additional fields as `Some`) is only used with new/compatible choices"
      )
      object DowngradeDropDefinedField
          extends ErrorCode(
            id = "INTERPRETATION_UPGRADE_ERROR_DOWNGRADE_DROP_DEFINED_FIELD",
            ErrorCategory.InvalidGivenCurrentSystemStateOther,
          ) {
        final case class Reject(
            override val cause: String,
            err: LfInterpretationError.Upgrade.DowngradeDropDefinedField,
        )(implicit
            loggingContext: ContextualizedErrorLogger
        ) extends DamlErrorWithDefiniteAnswer(
              cause = cause
            ) {
          override def resources: Seq[(ErrorResource, String)] =
            Seq(
              (ErrorResource.ExpectedType, err.expectedType.pretty),
              (ErrorResource.FieldIndex, err.fieldIndex.toString),
            )
        }
      }

      @Explanation(
        "An optional contract field with a value of Some may not be dropped during downgrading"
      )
      @Resolution(
        "There is data that is newer than the implementation using it, and thus is not compatible. Ensure new data (i.e. those with additional fields as `Some`) is only used with new/compatible choices"
      )
      object DowngradeFailed
          extends ErrorCode(
            id = "INTERPRETATION_UPGRADE_ERROR_DOWNGRADE_FAILED",
            ErrorCategory.InvalidGivenCurrentSystemStateOther,
          ) {
        final case class Reject(
            override val cause: String,
            err: LfInterpretationError.Upgrade.DowngradeFailed,
        )(implicit
            loggingContext: ContextualizedErrorLogger
        ) extends DamlErrorWithDefiniteAnswer(
              cause = cause
            ) {
          override def resources: Seq[(ErrorResource, String)] =
            Seq(
              (ErrorResource.ExpectedType, err.expectedType.pretty)
            )
        }
      }
    }

    @Explanation(
      """This error is a catch-all for errors thrown by in-development features, and should never be thrown in production."""
    )
    @Resolution(
      "See the error message for details of the specific in-development feature error. If this is production, avoid using development features."
    )
    object InterpretationDevError
        extends ErrorCode(
          id = "INTERPRETATION_DEV_ERROR",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.Dev.Error,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {

        override def resources: Seq[(ErrorResource, String)] =
          Seq(
            (ErrorResource.DevErrorType, err.getClass.getSimpleName)
          )
      }
    }
  }

  // TODO(#23334): Consider moving in dedicated error group
  @Explanation(
    """This error is a catch-all for errors thrown by topology-aware package selection in command processing."""
  )
  @Resolution(
    "Inspect the error message and adjust the topology state to ensure successful submissions"
  )
  object PackageSelectionFailed
      extends ErrorCode(
        id = "PACKAGE_SELECTION_FAILED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Reject(
        override val cause: String
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = cause
        ) {}
  }

  @Explanation(
    "A package-name required in command interpretation was discarded in topology-aware package selection."
  )
  @Resolution(
    "Revisit the command submission and ensure it conforms with the vetted topology state of the submitters and informees."
  )
  object PackageNameDiscarded
      extends ErrorCode(
        id = "PACKAGE_NAME_DISCARDED",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {

    final case class Reject(
        pkgName: Ref.PackageName,
        reference: Reference,
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            // TODO(#23334): Improve error reporting for package-name discarded errors
            s"Command interpretation failed due to the required $pkgName being discarded in package selection: $reference."
        )
  }
}
