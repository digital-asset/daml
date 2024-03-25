// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.definitions.{LedgerApiErrors}
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  ErrorGroup,
  ErrorResource,
  Explanation,
  Resolution,
}
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.{GlobalKey, TransactionVersion}
import com.daml.lf.value.{Value, ValueCoder}

@Explanation(
  "Errors raised during the command execution phase of the command submission evaluation."
)
object CommandExecution extends ErrorGroup()(LedgerApiErrors.errorClass) {
  def encodeValue(v: Value): Either[ValueCoder.EncodeError, String] =
    ValueCoder
      .encodeValue(TransactionVersion.VDev, v)
      .map(_.toStringUtf8)

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
    @Explanation(
      """This error occurs if an exercise or fetch happens on a transaction-locally consumed contract."""
    )
    @Resolution("This error indicates an application error.")
    object ContractNotActive
        extends ErrorCode(
          id = "CONTRACT_NOT_ACTIVE",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {

      final case class Reject(
          override val cause: String,
          err: LfInterpretationError.ContractNotActive,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = cause
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          (ErrorResource.TemplateId, err.templateId.toString),
          (ErrorResource.ContractId, err.coid.coid),
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
          ) {}
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
          ) {}
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
          ) {}
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

  }
}
