// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error

import com.digitalasset.base.error.{
  ContextualizedDamlError,
  ErrorCategory,
  ErrorCode,
  ErrorGroup,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.logging.ContextualizedErrorLogger
import com.digitalasset.daml.lf.archive.Error as LfArchiveError
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Error
import com.digitalasset.daml.lf.language.Util
import com.digitalasset.daml.lf.validation.{TypecheckUpgrades, UpgradeError}
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
      ) extends ContextualizedDamlError(
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
      ) extends ContextualizedDamlError(
            cause = "Dar file is corrupt",
            throwableO = Some(throwable),
            extraContext = Map(
              "entries" -> entries,
              "throwable" -> throwable,
            ),
          )
    }
    @Explanation(
      """The main package of the uploaded DAR does not match the expected package id."""
    )
    @Resolution(
      """Investigate where the DAR is coming from and whether it was manipulated or the provided package id was wrong."""
    )
    object MainPackageInDarDoesNotMatchExpected
        extends ErrorCode(
          id = "MAIN_PACKAGE_IN_DAR_DOES_NOT_MATCH_EXPECTED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {

      final case class Reject(found: String, expected: String)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends ContextualizedDamlError(
            cause =
              s"The main package of the uploaded DAR is '$found' while the provided expected value is '$expected'"
          )
    }

    @Explanation("""This error indicates that the supplied zipped dar file was invalid.""")
    @Resolution("Inspect the error message for details and contact support.")
    object InvalidZipEntry
        extends ErrorCode(id = "INVALID_ZIP_ENTRY", ErrorCategory.InvalidIndependentOfSystemState) {
      final case class Error(name: String, entries: Seq[String])(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends ContextualizedDamlError(
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
      ) extends ContextualizedDamlError(
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
      ) extends ContextualizedDamlError(
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
      ) extends ContextualizedDamlError(
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
    ) extends ContextualizedDamlError(
          cause = "Internal package validation error.",
          extraContext = Map(
            "nameOfFunc" -> nameOfFunc,
            "msg" -> msg,
            "detailMsg" -> detailMsg,
          ),
        )
    final case class Error(missing: Set[Ref.PackageRef])(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends ContextualizedDamlError(
          cause = "Failed to resolve package ids locally.",
          extraContext = Map("missing" -> missing),
        )
    final case class Generic(reason: String)(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends ContextualizedDamlError(
          cause = "Generic error (please check the reason string).",
          extraContext = Map("reason" -> reason),
        )
    final case class Unhandled(throwable: Throwable, additionalReason: Option[String] = None)(
        implicit val loggingContext: ContextualizedErrorLogger
    ) extends ContextualizedDamlError(
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
    ): ContextualizedDamlError =
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
    ): ContextualizedDamlError = err match {
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
      ) extends ContextualizedDamlError(
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
    ) extends ContextualizedDamlError(
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
      ) extends ContextualizedDamlError(
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
          oldPackage: Util.PkgIdWithNameAndVersion,
          newPackage: Util.PkgIdWithNameAndVersion,
          upgradeError: UpgradeError,
          phase: TypecheckUpgrades.UploadPhaseCheck,
      )(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends ContextualizedDamlError(cause = phase match {
            case TypecheckUpgrades.MaximalDarCheck =>
              s"Upgrade checks indicate that new package $newPackage cannot be an upgrade of existing package $oldPackage. Reason: ${upgradeError.prettyInternal}"
            case TypecheckUpgrades.MinimalDarCheck =>
              s"Upgrade checks indicate that existing package $newPackage cannot be an upgrade of new package $oldPackage. Reason: ${upgradeError.prettyInternal}"
          })
    }

    @Explanation(
      """This error indicates that a package with name daml-prim that isn't a utility package was uploaded. All daml-prim packages should be utility packages.""""
    )
    @Resolution("Contact the supplier of the Dar.")
    object UpgradeDamlPrimIsNotAUtilityPackage
        extends ErrorCode(
          id = "DAML_PRIM_NOT_UTILITY_PACKAGE",
          ErrorCategory.InvalidIndependentOfSystemState,
        ) {
      final case class Error(
          uploadedPackage: Util.PkgIdWithNameAndVersion
      )(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends ContextualizedDamlError(
            cause =
              s"Tried to upload a package $uploadedPackage, but this package is not a utility package. All packages named `daml-prim` must be a utility package.",
            extraContext = Map(
              "uploadedPackage" -> uploadedPackage
            ),
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
      @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
      final case class Error(
          uploadedPackage: Util.PkgIdWithNameAndVersion,
          existingPackage: Ref.PackageId,
          packageVersion: Ref.PackageVersion,
      )(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends ContextualizedDamlError(
            cause =
              s"Tried to upload package $uploadedPackage, but a different package $existingPackage with the same name and version has previously been uploaded.",
            extraContext = Map(
              "uploadedPackageId" -> uploadedPackage,
              "existingPackage" -> existingPackage,
              "packageVersion" -> packageVersion.toString,
            ),
          )
    }
  }
}
