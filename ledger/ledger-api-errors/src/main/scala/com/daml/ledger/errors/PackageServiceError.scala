// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.errors

import com.daml.error._

@Explanation(
  "Errors raised by the Package Management Service on package uploads."
)
object PackageServiceError extends LedgerApiErrors.PackageServiceErrorGroup {
  @Explanation("Package parsing errors raised during package upload.")
  object Reading extends ErrorGroup {
    @Explanation(
      """This error indicates that the supplied dar file name did not meet the requirements to be stored in the persistence store."""
    )
    @Resolution("Inspect error message for details and change the file name accordingly")
    object InvalidDarFileName
        extends ErrorCode(
          id = "INVALID_DAR_FILE_NAME",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(reason: String)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends DamlError(
            cause = "Dar file name is invalid",
            extraContext = Map("reason" -> reason),
          )
    }

    @Explanation("""This error indicates that the supplied dar file was invalid.""")
    @Resolution("Inspect the error message for details and contact support.")
    object InvalidDar
        extends ErrorCode(id = "INVALID_DAR", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(entries: Seq[String], throwable: Throwable)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends DamlError(
            cause = "Dar file is corrupt",
            throwableO = Some(throwable),
            extraContext = Map(
              "entries" -> entries,
              "throwable" -> throwable,
            ),
          )
    }
    @Explanation("""This error indicates that the supplied zipped dar file was invalid.""")
    @Resolution("Inspect the error message for details and contact support.")
    object InvalidZipEntry
        extends ErrorCode(id = "INVALID_ZIP_ENTRY", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(name: String, entries: Seq[String])(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends DamlError(
            cause = "Dar zip file is corrupt",
            extraContext = Map(
              "name" -> name,
              "entries" -> entries,
            ),
          )
    }

    @Explanation(
      """This error indicates that the supplied zipped dar is an unsupported legacy Dar."""
    )
    @Resolution("Please use a more recent dar version.")
    object InvalidLegacyDar
        extends ErrorCode(
          id = "INVALID_LEGACY_DAR",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(entries: Seq[String])(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends DamlError(
            cause = "Unsupported legacy Dar zip file",
            extraContext = Map("entries" -> entries),
          )
    }

    @Explanation("""This error indicates that the supplied zipped dar is regarded as zip-bomb.""")
    @Resolution("Inspect the dar and contact support.")
    object ZipBomb
        extends ErrorCode(id = "ZIP_BOMB", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(msg: String)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends DamlError(
            cause = "Dar zip file seems to be a zip bomb.",
            extraContext = Map("msg" -> msg),
          )
    }

    @Explanation(
      """This error indicates that the content of the Dar file could not be parsed successfully."""
    )
    @Resolution("Inspect the error message and contact support.")
    object ParseError
        extends ErrorCode(id = "DAR_PARSE_ERROR", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(reason: String)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends DamlError(
            cause = "Failed to parse the dar file content.",
            extraContext = Map(reason -> reason),
          )
    }

  }

  @Explanation("""This error indicates an internal issue within the package service.""")
  @Resolution("Inspect the error message and contact support.")
  object InternalError
      extends ErrorCode(
        id = "PACKAGE_SERVICE_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Validation(nameOfFunc: String, msg: String, detailMsg: String = "")(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = "Internal package validation error.",
          extraContext = Map(
            "nameOfFunc" -> nameOfFunc,
            "msg" -> msg,
            "detailMsg" -> detailMsg,
          ),
        )
    final case class Error(missing: Set[String])(
        implicit // PackageId
        val loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = "Failed to resolve package ids locally.",
          extraContext = Map("missing" -> missing),
        )
    final case class Generic(reason: String)(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = "Generic error (please check the reason string).",
          extraContext = Map("reason" -> reason),
        )
    final case class Unhandled(throwable: Throwable)(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = "Failed with an unknown error cause",
          throwableO = Some(throwable),
          extraContext = Map("throwable" -> throwable),
        )
  }

  object Validation {
    @Explanation("""This error indicates that the validation of the uploaded dar failed.""")
    @Resolution("Inspect the error message and contact support.")
    object ValidationError
        extends ErrorCode(
          id = "DAR_VALIDATION_ERROR",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(validationError: String)(
          implicit // validation.ValidationError
          val loggingContext: ContextualizedErrorLogger
      ) extends DamlError(
            cause = "Package validation failed.",
            extraContext = Map("validationError" -> validationError),
          )
    }

    final case class AllowedLanguageMismatchError(
//        packageId: Ref.PackageId,
//        languageVersion: language.LanguageVersion,
//        allowedLanguageVersions: VersionRange[language.LanguageVersion],
        override val cause: String,
        packageId: String,
        languageVersion: String,
        allowedLanguageVersions: String,
    )(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = cause,
          extraContext = Map(
            "packageId" -> packageId,
            "languageVersion" -> languageVersion,
            "allowedLanguageVersions" -> allowedLanguageVersions,
          ),
        )(
          LedgerApiErrors.CommandExecution.Package.AllowedLanguageVersions,
          loggingContext,
        ) // reuse error code of ledger api server

    @Explanation(
      """This error indicates that the uploaded Dar is broken because it is missing internal dependencies."""
    )
    @Resolution("Contact the supplier of the Dar.")
    object SelfConsistency
        extends ErrorCode(
          id = "DAR_NOT_SELF_CONSISTENT",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(
//          packageIds: Set[Ref.PackageId],
//          missingDependencies: Set[Ref.PackageId],
          packageIds: Set[String],
          missingDependencies: Set[String],
      )(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends DamlError(
            cause =
              "The set of packages in the dar is not self-consistent and is missing dependencies",
            extraContext = Map(
              "packageIds" -> packageIds,
              "missingDependencies" -> missingDependencies,
            ),
          )
    }
  }
}
