// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error.groups

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
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{LookupError, Reference}
import com.digitalasset.canton.ledger.error.LedgerApiErrors.{
  EarliestOffsetMetadataKey,
  LatestOffsetMetadataKey,
}
import com.digitalasset.canton.ledger.error.ParticipantErrorGroup.LedgerApiErrorGroup.RequestValidationErrorGroup

import java.time.Duration

@Explanation(
  "Validation errors raised when evaluating requests in the Ledger API."
)
object RequestValidationErrors extends RequestValidationErrorGroup {
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
      final case class Reject(packageId: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause = "Could not find package."
          ) {

        override def resources: Seq[(ErrorResource, String)] = {
          super.resources :+ ((ErrorResource.DalfPackage, packageId))
        }
      }

      final case class InterpretationReject(
          packageId: PackageId,
          reference: Reference,
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
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

      final case class Reject(transactionId: String)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = "Transaction not found, or not visible.") {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          (ErrorResource.TransactionId, transactionId)
        )
      }
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
          unknownTemplatesOrInterfaces: Seq[Either[Ref.Identifier, Ref.Identifier]]
      ): String = {
        val unknownTemplateIds =
          unknownTemplatesOrInterfaces.collect { case Left(id) => id.toString }
        val unknownInterfaceIds =
          unknownTemplatesOrInterfaces.collect { case Right(id) => id.toString }

        val templatesMessage = if (unknownTemplateIds.nonEmpty) {
          s"Templates do not exist: [${unknownTemplateIds.mkString(", ")}]. "
        } else ""
        val interfacesMessage = if (unknownInterfaceIds.nonEmpty) {
          s"Interfaces do not exist: [${unknownInterfaceIds.mkString(", ")}]. "
        } else
          ""
        (templatesMessage + interfacesMessage).trim
      }

      final case class Reject(
          unknownTemplatesOrInterfaces: Seq[Either[Ref.Identifier, Ref.Identifier]]
      )(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = buildCause(unknownTemplatesOrInterfaces)) {
        override def resources: Seq[(ErrorResource, String)] =
          unknownTemplatesOrInterfaces.map {
            case Left(templateId) => ErrorResource.TemplateId -> templateId.toString
            case Right(interfaceId) => ErrorResource.InterfaceId -> interfaceId.toString
          }
      }
    }

    @Explanation(
      "The queried package names do not match packages uploaded on this participant."
    )
    @Resolution(
      "Use valid package names or ask the participant operator to upload the necessary packages."
    )
    object PackageNamesNotFound
        extends ErrorCode(
          id = "PACKAGE_NAMES_NOT_FOUND",
          category = ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      final case class Reject(unknownPackageNames: Set[Ref.PackageName])(implicit
          contextualizedErrorLogger: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause =
              s"The following package names do not match upgradable packages uploaded on this participant: [${unknownPackageNames
                  .mkString(", ")}]."
          )
    }

    @Explanation(
      "The queried type reference for the specified package name and template qualified-name does not reference any template uploaded on this participant"
    )
    @Resolution(
      "Use a template qualified-name referencing already uploaded template-ids or ask the participant operator to upload the necessary packages."
    )
    object NoTemplatesForPackageNameAndQualifiedName
        extends ErrorCode(
          id = "NO_TEMPLATES_FOR_PACKAGE_NAME_AND_QUALIFIED_NAME",
          category = ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      final case class Reject(noKnownReferences: Set[(Ref.PackageName, Ref.QualifiedName)])(implicit
          contextualizedErrorLogger: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(
            cause =
              s"The following package-name/template qualified-name pairs do not reference any template-id uploaded on this participant: [${noKnownReferences
                  .mkString(", ")}]."
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
    final case class Reject(override val cause: String, earliestOffset: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = cause,
          extraContext = Map(EarliestOffsetMetadataKey -> earliestOffset),
        )
  }

  @Explanation(
    "This rejection is given when a read request tries to access data after the ledger end"
  )
  @Resolution("Use an offset that is before the ledger end.")
  object ParticipantDataAccessedAfterLedgerEnd
      extends ErrorCode(
        id = "PARTICIPANT_DATA_ACCESSED_AFTER_LEDGER_END",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(override val cause: String, latestOffset: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = cause,
          extraContext = Map(LatestOffsetMetadataKey -> latestOffset),
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
    final case class Reject(offsetType: String, requestedOffset: String, ledgerEnd: String)(implicit
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
    final case class Reject(message: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = message)
  }

  @Explanation(
    """This error is emitted when a mandatory field is not set in a submitted ledger API command."""
  )
  @Resolution("Inspect the reason given and correct your application.")
  object MissingField
      extends ErrorCode(id = "MISSING_FIELD", ErrorCategory.InvalidIndependentOfSystemState) {
    final case class Reject(missingField: String)(implicit
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
    final case class Reject(reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"The submitted request has invalid arguments: ${reason}"
        )
  }

  @Explanation(
    """This error is emitted when a submitted ledger API command contains a field value that cannot be understood."""
  )
  @Resolution("Inspect the reason given and correct your application.")
  object InvalidField
      extends ErrorCode(id = "INVALID_FIELD", ErrorCategory.InvalidIndependentOfSystemState) {
    final case class Reject(fieldName: String, message: String)(implicit
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
    final case class Reject(
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
    final case class Error(
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
