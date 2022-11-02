// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.errors.groups

import java.time.Duration
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlError,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  ErrorGroup,
  ErrorResource,
  Explanation,
  Resolution,
}
import com.daml.ledger.errors.LedgerApiErrors
import com.daml.ledger.errors.LedgerApiErrors.EarliestOffsetMetadataKey

@Explanation(
  "Validation errors raised when evaluating requests in the Ledger API."
)
object RequestValidation extends LedgerApiErrors.RequestValidation {
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
      ) extends DamlErrorWithDefiniteAnswer(
            cause = "Could not find package."
          ) {

        override def resources: Seq[(ErrorResource, String)] = {
          super.resources :+ ((ErrorResource.DalfPackage, packageId))
        }
      }

      case class InterpretationReject(
          override val cause: String
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = cause)
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

      case class Reject(transactionId: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = "Transaction not found, or not visible.") {
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
      ) extends DamlErrorWithDefiniteAnswer(
            cause = "The ledger configuration could not be retrieved."
          )

      case class RejectWithMessage(message: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = s"The ledger configuration could not be retrieved: ${message}."
          )
    }

    @Explanation(
      "The queried template or interface ids do not exist."
    )
    @Resolution(
      "Use valid template or interface ids in your query or ask the participant operator to upload the package containing the necessary interfaces/templates."
    )
    object TemplateOrInterfaceIdsNotFound
        extends ErrorCode(
          id = "TEMPLATES_OR_INTERFACES_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {

      private def buildCause(
          unknownTemplatesOrInterfaces: Seq[Either[String, String]]
      ): String = {
        val unknownTemplateIds =
          unknownTemplatesOrInterfaces.collect { case Left(id) => id }
        val unknownInterfaceIds =
          unknownTemplatesOrInterfaces.collect { case Right(id) => id }

        val templatesMessage = if (unknownTemplateIds.nonEmpty) {
          s"Templates do not exist: [${unknownTemplateIds.mkString(", ")}]. "
        } else ""
        val interfacesMessage = if (unknownInterfaceIds.nonEmpty) {
          s"Interfaces do not exist: [${unknownInterfaceIds.mkString(", ")}]. "
        } else
          ""
        (templatesMessage + interfacesMessage).trim
      }

      case class Reject(unknownTemplatesOrInterfaces: Seq[Either[String, String]])(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = buildCause(unknownTemplatesOrInterfaces)) {
        override def resources: Seq[(ErrorResource, String)] =
          unknownTemplatesOrInterfaces.map {
            case Left(templateId) => ErrorResource.TemplateId -> templateId.toString
            case Right(interfaceId) => ErrorResource.InterfaceId -> interfaceId.toString
          }
      }
    }
  }

  @Explanation("This rejection is given when a read request tries to access pruned data.")
  @Resolution("Use an offset that is after the pruning offset.")
  object ParticipantPrunedDataAccessed
      extends ErrorCode(
        id = "PARTICIPANT_PRUNED_DATA_ACCESSED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Reject(override val cause: String, earliestOffset: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = cause,
          extraContext = Map(EarliestOffsetMetadataKey -> earliestOffset),
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
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"${offsetType} offset (${requestedOffset}) is after ledger end (${ledgerEnd})"
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
    ) extends DamlErrorWithDefiniteAnswer(cause = message)
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
    case class Reject(expectedLedgerId: String, receivedLegerId: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"Ledger ID '${receivedLegerId}' not found. Actual Ledger ID is '${expectedLedgerId}'.",
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
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"The submitted command is missing a mandatory field: ${missingField}",
          extraContext = Map("field_name" -> missingField),
        )
  }

  @Explanation(
    """This error is emitted when a submitted ledger API command contains an invalid argument."""
  )
  @Resolution("Inspect the reason given and correct your application.")
  object InvalidArgument
      extends ErrorCode(id = "INVALID_ARGUMENT", ErrorCategory.InvalidIndependentOfSystemState) {
    case class Reject(reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          // TODO um-for-hub: Update the cause to mention a 'request' instead of a 'command'
          cause = s"The submitted command has invalid arguments: ${reason}"
        )
  }

  @Explanation(
    """This error is emitted when a submitted ledger API command contains a field value that cannot be understood."""
  )
  @Resolution("Inspect the reason given and correct your application.")
  object InvalidField
      extends ErrorCode(id = "INVALID_FIELD", ErrorCategory.InvalidIndependentOfSystemState) {
    case class Reject(fieldName: String, message: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The submitted command has a field with invalid value: Invalid field ${fieldName}: ${message}"
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
    val ValidMaxDeduplicationFieldKey = "longest_duration"
    case class Reject(
        reason: String,
        maxDeduplicationDuration: Option[Duration],
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"The submitted command had an invalid deduplication period: ${reason}"
        ) {
      override def context: Map[String, String] = {
        super.context ++ maxDeduplicationDuration
          .map(ValidMaxDeduplicationFieldKey -> _.toString)
          .toList
      }
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
        val loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = s"Offset in ${fieldName} not specified in hexadecimal: ${offsetValue}: ${message}"
        )
  }
}
