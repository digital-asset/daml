// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error._
import com.daml.lf.archive.{Error => LfArchiveError}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Error
import com.daml.lf.{VersionRange, language, validation}

trait PackageServiceError extends BaseError

abstract class LoggingPackageServiceError(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
)(implicit override val code: ErrorCode)
    extends BaseError.Impl(cause, throwableO)
    with PackageServiceError {
  final override def logOnCreation: Boolean = true
}

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
      ) extends LoggingPackageServiceError(
            cause = "Dar file name is invalid"
          )
    }

    @Explanation("""This error indicates that the supplied dar file was invalid.""")
    @Resolution("Inspect the error message for details and contact support.")
    object InvalidDar
        extends ErrorCode(id = "INVALID_DAR", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(entries: Seq[String], throwable: Throwable)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends LoggingPackageServiceError(
            cause = "Dar file is corrupt",
            throwableO = Some(throwable),
          )
    }
    @Explanation("""This error indicates that the supplied zipped dar file was invalid.""")
    @Resolution("Inspect the error message for details and contact support.")
    object InvalidZipEntry
        extends ErrorCode(id = "INVALID_ZIP_ENTRY", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(name: String, entries: Seq[String])(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends LoggingPackageServiceError(
            cause = "Dar zip file is corrupt"
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
      ) extends LoggingPackageServiceError(
            cause = "Unsupported legacy Dar zip file"
          )
    }

    @Explanation("""This error indicates that the supplied zipped dar is regarded as zip-bomb.""")
    @Resolution("Inspect the dar and contact support.")
    object ZipBomb
        extends ErrorCode(id = "ZIP_BOMB", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(msg: String)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends LoggingPackageServiceError(
            cause = "Dar zip file seems to be a zip bomb."
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
      ) extends LoggingPackageServiceError(
            cause = "Failed to parse the dar file content."
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
    ) extends LoggingPackageServiceError(
          cause = "Internal package validation error."
        )
    final case class Error(missing: Set[PackageId])(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends LoggingPackageServiceError(
          cause = "Failed to resolve package ids locally."
        )
    final case class Generic(reason: String)(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends LoggingPackageServiceError(
          cause = "Generic error (please check the reason string)."
        )
    final case class Unhandled(throwable: Throwable)(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends LoggingPackageServiceError(
          cause = "Failed with an unknown error cause",
          throwableO = Some(throwable),
        )
  }

  object Validation {
    def handleLfArchiveError(
        lfArchiveError: LfArchiveError
    )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): LoggingPackageServiceError =
      lfArchiveError match {
        case LfArchiveError.InvalidDar(entries, cause) =>
          PackageServiceError.Reading.InvalidDar
            .Error(entries.entries.keys.toSeq, cause)
        case LfArchiveError.InvalidZipEntry(name, entries) =>
          PackageServiceError.Reading.InvalidZipEntry
            .Error(name, entries.entries.keys.toSeq)
        case LfArchiveError.InvalidLegacyDar(entries) =>
          PackageServiceError.Reading.InvalidLegacyDar.Error(entries.entries.keys.toSeq)
        case LfArchiveError.ZipBomb =>
          PackageServiceError.Reading.ZipBomb.Error(LfArchiveError.ZipBomb.getMessage)
        case e: LfArchiveError =>
          PackageServiceError.Reading.ParseError.Error(e.msg)
        case e =>
          PackageServiceError.InternalError.Unhandled(e)
      }

    def handleLfEnginePackageError(err: Error.Package.Error)(implicit
        loggingContext: ContextualizedErrorLogger
    ): LoggingPackageServiceError = err match {
      case Error.Package.Internal(nameOfFunc, msg, _) =>
        PackageServiceError.InternalError.Validation(nameOfFunc, msg)
      case Error.Package.Validation(validationError) =>
        ValidationError.Error(validationError)
      case Error.Package.MissingPackage(packageId, _) =>
        PackageServiceError.InternalError.Error(Set(packageId))
      case Error.Package
            .AllowedLanguageVersion(packageId, languageVersion, allowedLanguageVersions) =>
        AllowedLanguageMismatchError(
          packageId,
          languageVersion,
          allowedLanguageVersions,
        )
      case Error.Package.SelfConsistency(packageIds, missingDependencies) =>
        SelfConsistency.Error(packageIds, missingDependencies)
    }

    @Explanation("""This error indicates that the validation of the uploaded dar failed.""")
    @Resolution("Inspect the error message and contact support.")
    object ValidationError
        extends ErrorCode(
          id = "DAR_VALIDATION_ERROR",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(validationError: validation.ValidationError)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends LoggingPackageServiceError(
            cause = "Package validation failed."
          )
    }

    final case class AllowedLanguageMismatchError(
        packageId: Ref.PackageId,
        languageVersion: language.LanguageVersion,
        allowedLanguageVersions: VersionRange[language.LanguageVersion],
    )(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends LoggingPackageServiceError(
          cause = LedgerApiErrors.CommandExecution.Package.AllowedLanguageVersions
            .buildCause(packageId, languageVersion, allowedLanguageVersions)
        )(
          LedgerApiErrors.CommandExecution.Package.AllowedLanguageVersions
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
          packageIds: Set[Ref.PackageId],
          missingDependencies: Set[Ref.PackageId],
      )(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends LoggingPackageServiceError(
            cause =
              "The set of packages in the dar is not self-consistent and is missing dependencies"
          )
    }
  }
}
