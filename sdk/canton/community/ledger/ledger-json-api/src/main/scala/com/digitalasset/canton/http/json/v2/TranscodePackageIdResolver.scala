// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import cats.data.EitherT
import cats.implicits.{toBifunctorOps, toTraverseOps}
import com.daml.ledger.api.v2 as lapi
import com.daml.ledger.api.v2.commands.Command
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.http.json.v2.TranscodePackageIdResolver.EitherOps
import com.digitalasset.canton.ledger.api.validation.GetPreferredPackagesRequestValidator.PackageVettingRequirements
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.{invalidField, missingField}
import com.digitalasset.canton.ledger.error.JsonApiErrors
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.PackagePreferenceBackend
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPartyId}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName}
import io.grpc.StatusRuntimeException

import scala.concurrent.{ExecutionContext, Future}

private[json] trait TranscodePackageIdResolver extends NamedLogging {
  implicit def ec: ExecutionContext

  /** Resolves the package-names of the given JSON Ledger API commands to package-ids, using
    * [[resolvePackageNamesInternal()]]
    */
  def resolveDecodingPackageIdsForJsonCommands(
      jsCommands: Seq[JsCommand.Command],
      actAs: Seq[String],
      packageIdSelectionPreference: Seq[String],
      synchronizerIdO: Option[String],
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[(JsCommand.Command, String)]] = {
    val commandsPackageIds = jsCommands.map {
      case cmd: JsCommand.CreateCommand => cmd.templateId.packageId
      case cmd: JsCommand.ExerciseCommand => cmd.templateId.packageId
      case cmd: JsCommand.CreateAndExerciseCommand => cmd.templateId.packageId
      case cmd: JsCommand.ExerciseByKeyCommand => cmd.templateId.packageId
    }

    for {
      submittingPartyStr <- actAs.headOption
        .map(Future.successful)
        .getOrElse(Future.failed(missingField("act_as/actAs")))
      packageResolutionMapO <- resolvePackageNames(
        commandsPackageIdsRaw = commandsPackageIds.toSet,
        packageIdSelectionPreferencesRaw = packageIdSelectionPreference.toSet,
        submittingPartyRaw = submittingPartyStr,
        synchronizerId = synchronizerIdO,
      )
      packageResolutionMap = packageResolutionMapO.getOrElse(Map.empty)

      // If the package-ids have package-name format, then they are in the packageResolutionMap,
      // so replace with the resolved package-id. Otherwise, keep as is.
      commandsWithDecodingPackages = jsCommands
        .map {
          case cmd: JsCommand.CreateCommand =>
            val cmdPkgId = cmd.templateId.packageId
            cmd -> packageResolutionMap.getOrElse(cmdPkgId, cmdPkgId)
          case cmd: JsCommand.ExerciseCommand =>
            val cmdPkgId = cmd.templateId.packageId
            cmd -> packageResolutionMap.getOrElse(cmdPkgId, cmdPkgId)
          case cmd: JsCommand.CreateAndExerciseCommand =>
            val cmdPkgId = cmd.templateId.packageId
            cmd -> packageResolutionMap.getOrElse(cmdPkgId, cmdPkgId)
          case cmd: JsCommand.ExerciseByKeyCommand =>
            val cmdPkgId = cmd.templateId.packageId
            cmd -> packageResolutionMap.getOrElse(cmdPkgId, cmdPkgId)
        }
    } yield commandsWithDecodingPackages
  }

  /** Resolves the package-names of the given gRPC Ledger API commands to package-ids, using
    * [[resolvePackageNamesInternal()]].
    *
    * Note: This direction of decoding is not used in the Ledger JSON API, but only in Ledger API
    * conformance test code
    */
  def resolveDecodingPackageIdsForGrpcCommands(
      grpcCommands: Seq[lapi.commands.Command.Command],
      actAs: Seq[String],
      packageIdSelectionPreference: Seq[String],
      synchronizerIdO: Option[String],
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[(Command.Command, String)]] = {
    val commandsPackageIds = grpcCommands.map {
      case lapi.commands.Command.Command.Empty => sys.error("Unexpected Empty command")
      case cmd: lapi.commands.Command.Command.Create =>
        cmd.value.templateId.getOrElse(sys.error("undefined template-id")).packageId
      case cmd: lapi.commands.Command.Command.Exercise =>
        cmd.value.templateId.getOrElse(sys.error("undefined template-id")).packageId
      case cmd: lapi.commands.Command.Command.ExerciseByKey =>
        cmd.value.templateId.getOrElse(sys.error("undefined template-id")).packageId
      case cmd: lapi.commands.Command.Command.CreateAndExercise =>
        cmd.value.templateId.getOrElse(sys.error("undefined template-id")).packageId
    }

    for {
      submittingPartyStr <- actAs.headOption
        .map(Future.successful)
        .getOrElse(Future.failed(missingField("act_as/actAs")))
      packageResolutionMapO <- resolvePackageNames(
        commandsPackageIdsRaw = commandsPackageIds.toSet,
        // TODO(#27500): Consider multiple preferences per package-name
        packageIdSelectionPreferencesRaw = packageIdSelectionPreference.toSet,
        submittingPartyRaw = submittingPartyStr,
        synchronizerId = synchronizerIdO,
      )
      packageResolutionMap = packageResolutionMapO.getOrElse(Map.empty)

      commandsWithDecodingPackages = grpcCommands.view.map {
        case lapi.commands.Command.Command.Empty => sys.error("Unexpected Empty command")
        case cmd: lapi.commands.Command.Command.Create =>
          val cmdPkgId =
            cmd.value.templateId
              .getOrElse(sys.error(s"Missing template-id in command $cmd"))
              .packageId
          cmd -> packageResolutionMap.getOrElse(cmdPkgId, cmdPkgId)
        case cmd: lapi.commands.Command.Command.Exercise =>
          val cmdPkgId =
            cmd.value.templateId
              .getOrElse(sys.error(s"Missing template-id in command $cmd"))
              .packageId
          cmd -> packageResolutionMap.getOrElse(cmdPkgId, cmdPkgId)
        case cmd: lapi.commands.Command.Command.ExerciseByKey =>
          val cmdPkgId =
            cmd.value.templateId
              .getOrElse(sys.error(s"Missing template-id in command $cmd"))
              .packageId
          cmd -> packageResolutionMap.getOrElse(cmdPkgId, cmdPkgId)
        case cmd: lapi.commands.Command.Command.CreateAndExercise =>
          val cmdPkgId =
            cmd.value.templateId
              .getOrElse(sys.error(s"Missing template-id in command $cmd"))
              .packageId
          cmd -> packageResolutionMap.getOrElse(cmdPkgId, cmdPkgId)
      }.toSeq
    } yield commandsWithDecodingPackages
  }

  protected def resolvePackageNamesInternal(
      packageNames: NonEmpty[Set[LfPackageName]],
      party: LfPartyId,
      packageIdSelectionPreferences: Set[LfPackageId],
      synchronizerIdO: Option[String],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfPackageName, LfPackageId]]

  private def resolvePackageNames(
      commandsPackageIdsRaw: Set[String],
      packageIdSelectionPreferencesRaw: Set[String],
      submittingPartyRaw: String,
      synchronizerId: Option[String],
  )(implicit
      traceContext: TraceContext
  ): Future[Option[Map[String, LfPackageId]]] = {

    val resultE = for {
      submittingParty <- LfPartyId
        .fromString(submittingPartyRaw)
        .left
        .map(err => ValidationErrors.invalidArgument(s"Invalid party in actAs/act_as: $err"))
      packageIdSelectionPreferences <- packageIdSelectionPreferencesRaw.toList
        .traverse(LfPackageId.fromString)
        .left
        .map(err => ValidationErrors.invalidArgument(s"packageIdSelectionPreferences: $err"))
      commandsPackageRefs <- commandsPackageIdsRaw.toList.traverse(
        Ref.PackageRef
          .fromString(_)
          .left
          .map(err =>
            ValidationErrors.invalidArgument(
              s"Value does not match the package-id or package-name formats: $err"
            )
          )
      )
      commandPackageNames =
        commandsPackageRefs.collect { case Ref.PackageRef.Name(name) => name }
    } yield (
      submittingParty,
      packageIdSelectionPreferences,
      commandPackageNames,
    )

    val resultFUS = for {
      (submittingParty, packageIdSelectionPreferences, commandPackageNames) <-
        resultE.toFutureUnlessShutdown
      packageResolutionMap <- NonEmpty
        .from(commandPackageNames)
        .traverse { nonEmptyPackageNames =>
          resolvePackageNamesInternal(
            packageNames = nonEmptyPackageNames.toSet,
            party = submittingParty,
            packageIdSelectionPreferences = packageIdSelectionPreferences.toSet,
            synchronizerIdO = synchronizerId,
          )
        }
    } yield packageResolutionMap.map(_.map { case (pkgName, id) =>
      Ref.PackageRef.Name(pkgName).toString() -> id
    })

    resultFUS.asGrpcFuture
  }
}

private class TranscodeTopologyAwarePackageBackedResolver(
    packagePreferenceBackend: PackagePreferenceBackend,
    fetchPackageMetadataSnapshot: ErrorLoggingContext => PackageMetadata,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends TranscodePackageIdResolver
    with NamedLogging {

  /** Finds a package-id vetted by the provided party for each of the given package-names. This
    * resolver is used to find a suitable package for decoding Daml arguments provided in JSON API
    * commands, when the package-id is not explicitly provided in the command's template-id (i.e. it
    * uses a package-name scoped identifier).
    *
    * @param packageNames
    *   The package-names to resolve.
    * @param party
    *   The party for which the package-names should be resolved.
    * @param packageIdSelectionPreferences
    *   The package-ids that should act as a restriction for the selection. A package-id restricts
    *   the selection if its package-name is in `packageNames`.
    * @param synchronizerIdO
    *   If provided, the synchronizer-id the resolution should be restricted to
    * @return
    *   A mapping from package-name to package-id for each of the given package-names. If no
    *   suitable selection could be found that satisfies all the given package-names, the error
    *   description is encoded in a failed Future.
    */
  override protected def resolvePackageNamesInternal(
      packageNames: NonEmpty[Set[LfPackageName]],
      party: LfPartyId,
      packageIdSelectionPreferences: Set[LfPackageId],
      synchronizerIdO: Option[String],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfPackageName, LfPackageId]] = {
    val packageMetadataSnapshot = fetchPackageMetadataSnapshot(implicitly[ErrorLoggingContext])
    for {
      userPackagePreferenceMap <-
        TranscodePackageIdResolver
          .resolvePackageNames(packageIdSelectionPreferences, packageMetadataSnapshot)
          .leftMap(JsonApiErrors.JsonApiPackageSelectionFailed.Reject(_).asGrpcError)
          .toFutureUnlessShutdown
      synchronizerIdO <- synchronizerIdO.traverse { id =>
        SynchronizerId.fromString(id).left.map(invalidField("synchronizer_id", _))
      }.toFutureUnlessShutdown
      result <- EitherT(
        packagePreferenceBackend.getPreferredPackages(
          packageVettingRequirements =
            PackageVettingRequirements(packageNames.iterator.map(_ -> Set(party)).toMap),
          packageFilter = PackagePreferenceBackend.SupportedPackagesFilter(
            supportedPackagesPerPackageName = userPackagePreferenceMap.view.mapValues(Set(_)).toMap,
            restrictionDescription = "Commands.packageIdSelectionPreferences",
          ),
          synchronizerId = synchronizerIdO,
          vettingValidAt = None,
        )
      ).leftMap(JsonApiErrors.JsonApiPackageSelectionFailed.Reject(_).asGrpcError)
        .leftSemiflatMap(FutureUnlessShutdown.failed)
        .merge
      (preferences, synchronizerId) = result
    } yield {
      logger.debug(
        show"Computed package-preferences $preferences for synchronizer $synchronizerId for JSON API request Daml decoding"
      )
      preferences.view.map(pkgRef => pkgRef.packageName -> pkgRef.pkgId).toMap
    }
  }
}

/** Only used for testing purposes */
private class TranscodePackageMetadataBackedResolver(
    fetchPackageMetadataSnapshot: () => PackageMetadata,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends TranscodePackageIdResolver
    with NamedLogging {

  override protected def resolvePackageNamesInternal(
      packageNames: NonEmpty[Set[LfPackageName]],
      party: LfPartyId,
      userPreferences: Set[LfPackageId],
      synchronizerIdO: Option[String], // Not used in this implementation
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfPackageName, LfPackageId]] = {
    val packageMetadataSnapshot = fetchPackageMetadataSnapshot()
    val packageNameMap = packageMetadataSnapshot.packageNameMap

    (for {
      userPreferences <-
        TranscodePackageIdResolver.resolvePackageNames(userPreferences, packageMetadataSnapshot)
      localPreferences <- packageNames.forgetNE.toList.traverse { packageName =>
        packageNameMap
          .get(packageName)
          .map(resolution => packageName -> resolution.preference.packageId)
          .toRight(show"Package-name $packageName not known")
      }
    } yield {
      // User preferences take precedence over local preferences
      // It is fine to assume that the userPreferences are backed by existing packages,
      // since this is used in test code only.
      localPreferences.toMap ++ userPreferences
    })
      .leftMap(err => JsonApiErrors.JsonApiPackageSelectionFailed.Reject(err).asGrpcError)
      .toFutureUnlessShutdown
  }
}

object TranscodePackageIdResolver {
  def topologyStateBacked(
      packagePreferenceBackend: PackagePreferenceBackend,
      fetchPackageMetadataSnapshot: ErrorLoggingContext => PackageMetadata,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): TranscodePackageIdResolver =
    new TranscodeTopologyAwarePackageBackedResolver(
      packagePreferenceBackend,
      fetchPackageMetadataSnapshot,
      loggerFactory,
    )

  /** Only used for testing purposes */
  def packageMetadataBacked(
      fetchPackageMetadataSnapshot: () => PackageMetadata,
      namedLoggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): TranscodePackageIdResolver = new TranscodePackageMetadataBackedResolver(
    fetchPackageMetadataSnapshot,
    namedLoggerFactory,
  )

  private[v2] def resolvePackageNames(
      packageIdSelectionPreferences: Set[LfPackageId],
      packageMetadataSnapshot: PackageMetadata,
  ): Either[String, Map[PackageName, LfPackageId]] = {
    val packageIndex: Map[PackageId, (PackageName, Ref.PackageVersion)] =
      packageMetadataSnapshot.packageIdVersionMap

    packageIdSelectionPreferences.toList
      .traverse { packageId =>
        packageIndex
          .get(packageId)
          .map(_._1)
          .toRight(show"Package-id $packageId not known")
          .map(_ -> packageId)
      }
      // TODO(#27500): support multiple preferences per package-name
      .map(_.toMap)
  }

  private[json] implicit class EitherOps[R](val either: Either[StatusRuntimeException, R])
      extends AnyVal {
    def toFutureUnlessShutdown(implicit ec: ExecutionContext): FutureUnlessShutdown[R] =
      EitherT
        .fromEither[FutureUnlessShutdown](either)
        .leftSemiflatMap(FutureUnlessShutdown.failed)
        .merge
  }
}
