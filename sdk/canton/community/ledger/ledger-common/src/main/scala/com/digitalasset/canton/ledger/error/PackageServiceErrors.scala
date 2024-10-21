// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error

import com.daml.error.*
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.daml.lf.archive.Error as LfArchiveError
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Error
import com.digitalasset.daml.lf.validation.UpgradeError
import com.digitalasset.daml.lf.{VersionRange, language, validation}

import ParticipantErrorGroup.LedgerApiErrorGroup.PackageServiceErrorGroup

@Explanation(
  "Errors raised by the Package Management Service on package uploads."
)
object PackageServiceErrors extends PackageServiceErrorGroup {
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
    final case class Error(missing: Set[Ref.PackageRef])(implicit
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
    final case class Unhandled(throwable: Throwable, additionalReason: Option[String] = None)(
        implicit val loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = "Failed with an unknown error cause",
          throwableO = Some(throwable),
          extraContext = {
            val context: Map[String, Any] = Map("throwable" -> throwable)
            additionalReason match {
              case Some(additionalReason) => context + ("additionalReason" -> additionalReason)
              case None => context
            }
          },
        )
  }

  object Validation {
    def handleLfArchiveError(
        lfArchiveError: LfArchiveError
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): DamlError =
      lfArchiveError match {
        case LfArchiveError.InvalidDar(entries, cause) =>
          PackageServiceErrors.Reading.InvalidDar
            .Error(entries.entries.keys.toSeq, cause)
        case LfArchiveError.InvalidZipEntry(name, entries) =>
          PackageServiceErrors.Reading.InvalidZipEntry
            .Error(name, entries.entries.keys.toSeq)
        case LfArchiveError.InvalidLegacyDar(entries) =>
          PackageServiceErrors.Reading.InvalidLegacyDar.Error(entries.entries.keys.toSeq)
        case LfArchiveError.ZipBomb =>
          PackageServiceErrors.Reading.ZipBomb.Error(LfArchiveError.ZipBomb.getMessage)
        case e: LfArchiveError =>
          PackageServiceErrors.Reading.ParseError.Error(e.msg)
        case e =>
          PackageServiceErrors.InternalError.Unhandled(e)
      }

    def handleLfEnginePackageError(err: Error.Package.Error)(implicit
        loggingContext: ContextualizedErrorLogger
    ): DamlError = err match {
      case Error.Package.Internal(nameOfFunc, msg, _) =>
        PackageServiceErrors.InternalError.Validation(nameOfFunc, msg)
      case Error.Package.Validation(validationError) =>
        ValidationError.Error(validationError)
      case Error.Package.MissingPackage(packageRef, _) =>
        PackageServiceErrors.InternalError.Error(Set(packageRef))
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
      ) extends DamlError(
            cause = "Package validation failed.",
            extraContext = Map("validationError" -> validationError),
          )
    }

    final case class AllowedLanguageMismatchError(
        packageId: Ref.PackageId,
        languageVersion: language.LanguageVersion,
        allowedLanguageVersions: VersionRange[language.LanguageVersion],
    )(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = CommandExecutionErrors.Package.AllowedLanguageVersions
            .buildCause(packageId, languageVersion, allowedLanguageVersions),
          extraContext = Map(
            "packageId" -> packageId,
            "languageVersion" -> languageVersion.toString,
            "allowedLanguageVersions" -> allowedLanguageVersions.toString,
          ),
        )(
          CommandExecutionErrors.Package.AllowedLanguageVersions,
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
          packageIds: Set[Ref.PackageId],
          missingDependencies: Set[Ref.PackageId],
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

    @Explanation(
      """This error indicates that the uploaded Dar is invalid because it doesn't upgrade the packages it claims to upgrade."""
    )
    @Resolution("Contact the supplier of the Dar.")
    object Upgradeability
        extends ErrorCode(
          id = "DAR_NOT_VALID_UPGRADE",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(
          upgradingPackage: Ref.PackageId,
          upgradedPackage: Ref.PackageId,
          upgradeError: UpgradeError,
      )(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends DamlError(
            cause =
              s"The DAR contains a package which claims to upgrade another package, but basic checks indicate the package is not a valid upgrade. Upgrading package: $upgradingPackage; Upgraded package: $upgradedPackage; Reason: ${upgradeError.prettyInternal}"
          )
    }

    @Explanation(
      """This error indicates that the Dar upload failed upgrade checks because a package with the same version and package name has been previously uploaded."""
    )
    @Resolution("Inspect the error message and contact support.")
    object UpgradeVersion
        extends ErrorCode(
          id = "KNOWN_DAR_VERSION",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(
          uploadedPackageId: Ref.PackageId,
          existingPackage: Ref.PackageId,
          packageVersion: Ref.PackageVersion,
      )(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends DamlError(
            cause = "A DAR with the same version number has previously been uploaded.",
            extraContext = Map(
              "uploadedPackageId" -> uploadedPackageId,
              "existingPackage" -> existingPackage,
              "packageVersion" -> packageVersion.toString,
            ),
          )
    }
  }
}
