// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import cats.data.EitherT

import com.daml.error._
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Error
import com.daml.lf.{VersionRange, language, validation}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.error.ErrorGroups.ParticipantErrorGroup.PackageServiceErrorGroup
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

sealed trait PackageServiceError extends BaseError
object PackageServiceError extends PackageServiceErrorGroup {

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
          val logger: ContextualizedLogger,
          val loggingContext: LoggingContext,
          val correlationId: CorrelationId,
      ) extends BaseError.Impl(
            cause = "Dar file name is invalid",
            correlationId = correlationId.id,
          )
          with PackageServiceError

    }

    @Explanation("""This error indicates that the supplied dar file was invalid.""")
    @Resolution("Inspect the error message for details and contact support.")
    object InvalidDar
        extends ErrorCode(id = "INVALID_DAR", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(entries: Seq[String], throwable: Throwable)(implicit
          val logger: ContextualizedLogger,
          val loggingContext: LoggingContext,
          val correlationId: CorrelationId,
      ) extends BaseError.Impl(
            cause = "Dar file is corrupt",
            correlationId = correlationId.id,
            throwableO = Some(throwable),
          )
          with PackageServiceError
    }
    @Explanation("""This error indicates that the supplied zipped dar file was invalid.""")
    @Resolution("Inspect the error message for details and contact support.")
    object InvalidZipEntry
        extends ErrorCode(id = "INVALID_ZIP_ENTRY", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(name: String, entries: Seq[String])(implicit
          val logger: ContextualizedLogger,
          val loggingContext: LoggingContext,
          val correlationId: CorrelationId,
      ) extends BaseError.Impl(
            cause = "Dar zip file is corrupt",
            correlationId = correlationId.id,
          )
          with PackageServiceError
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
          val logger: ContextualizedLogger,
          val loggingContext: LoggingContext,
          val correlationId: CorrelationId,
      ) extends BaseError.Impl(
            cause = "Unsupported legacy Dar zip file",
            correlationId = correlationId.id,
          )
          with PackageServiceError
    }

    @Explanation("""This error indicates that the supplied zipped dar is regarded as zip-bomb.""")
    @Resolution("Inspect the dar and contact support.")
    object ZipBomb
        extends ErrorCode(id = "ZIP_BOMB", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(msg: String)(implicit
          val logger: ContextualizedLogger,
          val loggingContext: LoggingContext,
          val correlationId: CorrelationId,
      ) extends BaseError.Impl(
            cause = "Dar zip file seems to be a zip bomb.",
            correlationId = correlationId.id,
          )
          with PackageServiceError
    }

    @Explanation(
      """This error indicates that the content of the Dar file could not be parsed successfully."""
    )
    @Resolution("Inspect the error message and contact support.")
    object ParseError
        extends ErrorCode(id = "DAR_PARSE_ERROR", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(reason: String)(implicit
          val logger: ContextualizedLogger,
          val loggingContext: LoggingContext,
          val correlationId: CorrelationId,
      ) extends BaseError.Impl(
            cause = "Failed to parse the dar file content.",
            correlationId = correlationId.id,
          )
          with PackageServiceError
    }

  }

  @Explanation("""This error indicates an internal issue within the package service.""")
  @Resolution("Inspect the error message and contact support.")
  object InternalError
      extends ErrorCode(
        id = "PACKAGE_SERVICE_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class DamleNotSet()(implicit
        val logger: ContextualizedLogger,
        val loggingContext: LoggingContext,
        val correlationId: CorrelationId,
    ) extends BaseError.Impl(
          cause = "Package service is not initialised (misses damle).",
          correlationId = correlationId.id,
        )
        with PackageServiceError
    final case class Validation(nameOfFunc: String, msg: String, detailMsg: String = "")(implicit
        val logger: ContextualizedLogger,
        val loggingContext: LoggingContext,
        val correlationId: CorrelationId,
    ) extends BaseError.Impl(
          cause = "Internal package validation error.",
          correlationId = correlationId.id,
        )
        with PackageServiceError
    final case class Error(missing: Set[PackageId])(implicit
        val logger: ContextualizedLogger,
        val loggingContext: LoggingContext,
        val correlationId: CorrelationId,
    ) extends BaseError.Impl(
          cause = "Failed to resolve package ids locally.",
          correlationId = correlationId.id,
        )
        with PackageServiceError
    final case class Generic(reason: String)(implicit
        val logger: ContextualizedLogger,
        val loggingContext: LoggingContext,
        val correlationId: CorrelationId,
    ) extends BaseError.Impl(
          cause = "Generic error (please check the reason string).",
          correlationId = correlationId.id,
        )
        with PackageServiceError
    final case class Unhandled(throwable: Throwable)(implicit
        val logger: ContextualizedLogger,
        val loggingContext: LoggingContext,
        val correlationId: CorrelationId,
    ) extends BaseError.Impl(
          cause = "Failed with an unknown error cause",
          correlationId = correlationId.id,
          throwableO = Some(throwable),
        )
        with PackageServiceError
    @VisibleForTesting
    final case class Test(reason: String)(implicit
        val logger: ContextualizedLogger,
        val loggingContext: LoggingContext,
        val correlationId: CorrelationId,
    ) extends BaseError.Impl(
          cause = "A testing error.",
          correlationId = correlationId.id,
        )
        with PackageServiceError {
      override def logOnCreation: Boolean = false
    }
  }

  object Validation {

    def fromUpstreamError(err: Either[Error.Package.Error, Unit])(implicit
        logger: ContextualizedLogger,
        loggingContext: LoggingContext,
        correlationId: CorrelationId,
        executionContext: ExecutionContext,
    ): EitherT[Future, PackageServiceError, Unit] =
      EitherT.fromEither[Future](err.left.map {
        case Error.Package.Internal(nameOfFunc, msg) =>
          PackageServiceError.InternalError.Validation(nameOfFunc, msg)
        case Error.Package.Validation(validationError) =>
          ValidationError.Error(validationError)
        case Error.Package.MissingPackage(packageId, _) =>
          PackageServiceError.InternalError.Error(Set(packageId))
        case Error.Package
              .AllowedLanguageVersion(packageId, languageVersion, allowedLanguageVersions) =>
          AllowedLanguageMismatchError(packageId, languageVersion, allowedLanguageVersions)
        case Error.Package.SelfConsistency(packageIds, missingDependencies) =>
          SelfConsistency.Error(packageIds, missingDependencies)
      })

    @Explanation("""This error indicates that the validation of the uploaded dar failed.""")
    @Resolution("Inspect the error message and contact support.")
    object ValidationError
        extends ErrorCode(
          id = "DAR_VALIDATION_ERROR",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(validationError: validation.ValidationError)(implicit
          val logger: ContextualizedLogger,
          val loggingContext: LoggingContext,
          val correlationId: CorrelationId,
      ) extends BaseError.Impl(
            cause = "Package validation failed.",
            correlationId = correlationId.id,
          )
          with PackageServiceError
    }

    final case class AllowedLanguageMismatchError(
        packageId: Ref.PackageId,
        languageVersion: language.LanguageVersion,
        allowedLanguageVersions: VersionRange[language.LanguageVersion],
    )(implicit
        val logger: ContextualizedLogger,
        val loggingContext: LoggingContext,
        val correlationId: CorrelationId,
    ) extends BaseError.Impl(
          cause = LedgerApiErrors.Package.AllowedLanguageVersions
            .buildCause(packageId, languageVersion, allowedLanguageVersions),
          correlationId = correlationId.id,
        )(LedgerApiErrors.Package.AllowedLanguageVersions) // reuse error code of ledger api server
        with PackageServiceError

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
          val logger: ContextualizedLogger,
          val loggingContext: LoggingContext,
          val correlationId: CorrelationId,
      ) extends BaseError.Impl(
            cause =
              "The set of packages in the dar is not self-consistent and is missing dependencies",
            correlationId = correlationId.id,
          )
          with PackageServiceError
    }

  }

}
