// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import cats.implicits.{catsSyntaxAlternativeSeparate, toFoldableOps, toTraverseOps}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.ledger.api.{Commands, PackageReference}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.participant.state.{RoutingSynchronizerState, SyncService}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.PackagePreferenceBackend
import com.digitalasset.canton.platform.PackagePreferenceBackend.{
  Candidate,
  SortedPreferences,
  SupportedPackagesFilter,
}
import com.digitalasset.canton.platform.apiserver.execution.TapsCommandExecutionFactory.{
  PackagesForName,
  TapsDescription,
}
import com.digitalasset.canton.platform.apiserver.execution.TapsResult.{
  TapsPassExecutionResult,
  TapsPassInterpretationFailed,
}
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.apiserver.services.ErrorCause.RoutingFailed
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPackageVersion, LfPartyId}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
import com.digitalasset.daml.lf.engine.Error.{Package, Preprocessing}
import io.grpc.StatusRuntimeException

import scala.collection.MapView
import scala.collection.immutable.SortedSet
import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

import PackageReference.PackageReferenceOps

/** Factory for creating and chaining a series of TAPS steps to be used in a command execution (see
  * [[TopologyAwareCommandExecutor]]).
  *
  * Note: All the submission-related context parameters are shared across all TAPS passes created by
  * this factory, including the execution and logging contexts . Do not reuse objects of this class
  * across different submissions.
  */
private[execution] class TapsCommandExecutionFactory(
    commands: Commands,
    commandInterpreter: CommandInterpreter,
    forExternallySigned: Boolean,
    val loggerFactory: NamedLoggerFactory,
    packageMetadataSnapshot: PackageMetadata,
    rootLevelPackageNames: Set[LfPackageName],
    routingSynchronizerState: RoutingSynchronizerState,
    submissionSeed: Hash,
    syncService: SyncService,
)(implicit ec: ExecutionContext, loggingContextWithTrace: LoggingContextWithTrace)
    extends NamedLogging {

  private val userSpecifiedPreference: PackagesForName =
    orderUserSpecifiedPreferences(
      pkgIds = commands.packagePreferenceSet,
      packageVersionMap = packageMetadataSnapshot.packageIdVersionMap,
    )

  /** Models a single TAPS pass (see [[TopologyAwareCommandExecutor]] ScalaDoc for more details)
    *
    * @param tapsPassDescription
    *   The description of this TAPS pass (used for logging)
    * @param computeRequiredSubmitters
    *   The parties expected to require submission rights on the preparing participant on the
    *   selected synchronizer
    * @param computePartyPackageRequirements
    *   The package-names required to be vetted by each transaction informee party involved in the
    *   command. Only root-level package-names introduce strict requirements for the transaction's
    *   informees. Other package-names appearing in non-root nodes are only evaluated for debugging
    *   purposes (if their restriction cannot be satisfied, a debug log is emitted, but the
    *   synchronizer is not discarded).
    * @param computePackagePreferenceSet
    *   Computes the package preference set to be used for interpreting the command, given the
    *   per-synchronizer package preference sets and the pass input
    */
  class TapsPass[PassInput](
      val tapsPassDescription: String,
      computeRequiredSubmitters: PassInput => Set[Party],
      computePartyPackageRequirements: PassInput => Map[LfPartyId, Set[LfPackageName]],
      computePackagePreferenceSet: (
          PassInput,
          NonEmpty[Map[PhysicalSynchronizerId, Set[LfPackageId]]],
      ) => FutureUnlessShutdown[Set[LfPackageId]],
  ) extends NamedLogging {

    override protected def loggerFactory: NamedLoggerFactory =
      TapsCommandExecutionFactory.this.loggerFactory

    def execute(passInput: PassInput): FutureUnlessShutdown[TapsResult] = {
      val requiredSubmitters = computeRequiredSubmitters(passInput)
      val partyPackageRequirements = computePartyPackageRequirements(passInput)

      logDebug(s"Attempting $tapsPassDescription of $TapsDescription")
      val resultEFUS = for {
        // Compute package preference set
        perSynchronizerPreferenceSet <- EitherT.right(
          computePerSynchronizerPackagePreferenceSet(requiredSubmitters, partyPackageRequirements)
        )
        packagePreferenceSet <- EitherT
          .right(computePackagePreferenceSet(passInput, perSynchronizerPreferenceSet))
        _ = logDebug(show"Using package preference set: $packagePreferenceSet")

        // Interpret command with the computed package preference set
        commandInterpretationResult <- EitherT(
          commandInterpreter
            .interpret(
              commands.copy(packagePreferenceSet = packagePreferenceSet),
              submissionSeed,
            )
        )
          .leftMap(refinePackageNotFoundError(_, packageMetadataSnapshot.packageNameMap.keySet))
          .leftMap(TapsPassInterpretationFailed(_))

        // Try to route the interpreted command
        passResult <- syncService
          .selectRoutingSynchronizer(
            submitterInfo = commandInterpretationResult.submitterInfo,
            transaction = commandInterpretationResult.transaction,
            transactionMeta = commandInterpretationResult.transactionMeta,
            disclosedContractIds =
              commandInterpretationResult.processedDisclosedContracts.map(_.contractId).toList,
            optSynchronizerId = commandInterpretationResult.optSynchronizerId,
            transactionUsedForExternalSigning = forExternallySigned,
            routingSynchronizerState = routingSynchronizerState,
          )
          .map[TapsPassExecutionResult] { synchronizerRank =>
            // Pass succeeded - return the command execution result
            TapsPassExecutionResult.Succeeded(
              commandInterpretationResult.toCommandExecutionResult(
                synchronizerRank,
                routingSynchronizerState,
              )
            )
          }
          .leftFlatMap[TapsPassExecutionResult, TapsPassInterpretationFailed](err =>
            // Pass failed at routing stage - return the command interpretation result
            EitherT.rightT[FutureUnlessShutdown, TapsPassInterpretationFailed](
              TapsPassExecutionResult.RoutingFailed(
                commandInterpretationResult,
                ErrorCause.RoutingFailed(err),
              )
            )
          )
      } yield passResult

      resultEFUS.value.map(new TapsResult(_, logDebug(_)))
    }

    private def computePerSynchronizerPackagePreferenceSet(
        requiredSubmitters: Set[Party],
        partyPackageRequirements: Map[LfPartyId, Set[LfPackageName]],
    )(implicit
        loggingContextWithTrace: LoggingContextWithTrace
    ): FutureUnlessShutdown[NonEmpty[Map[PhysicalSynchronizerId, Set[LfPackageId]]]] =
      for {
        partyVettingMap: Map[PhysicalSynchronizerId, Map[LfPartyId, Set[PackageId]]] <-
          syncService.computePartyVettingMap(
            submitters =
              Option.unless(forExternallySigned)(requiredSubmitters).getOrElse(Set.empty),
            informees = partyPackageRequirements.keySet,
            vettingValidityTimestamp = CantonTimestamp(commands.submittedAt),
            prescribedSynchronizer = commands.synchronizerId,
            routingSynchronizerState = routingSynchronizerState,
          )

        _ = logDebug(
          show"Computing per-synchronizer package preference sets using the party-package requirements ($partyPackageRequirements) and root package-names ($rootLevelPackageNames)"
        )

        perSynchronizerCandidates: Map[PhysicalSynchronizerId, MapView[LfPackageName, Candidate[
          SortedPreferences
        ]]] =
          PackagePreferenceBackend
            .computePerSynchronizerPackageCandidates(
              synchronizersPartiesVettingState = partyVettingMap,
              packageMetadataSnapshot = packageMetadataSnapshot,
              packageFilter = SupportedPackagesFilter(
                supportedPackagesPerPackageName =
                  userSpecifiedPreference.view.mapValues(_.map(_.pkgId).toSet).toMap,
                restrictionDescription = "Commands.package_id_selection_preference",
              ),
              requirements = partyPackageRequirements,
              logger = logger,
            )

        (discardedSyncs, availableSyncs) =
          applyRootPackageNamesRestriction(perSynchronizerCandidates, rootLevelPackageNames)
            .map { case (sync, candidates) => candidates.map(sync -> _).left.map(sync -> _) }
            .toSeq
            .separate

        perSynchronizerPreferenceSet <-
          NonEmpty
            .from(availableSyncs.toMap)
            .toRight(
              buildSelectionFailedError(
                prescribedSynchronizerIdO = commands.synchronizerId,
                discardedSynchronizers = discardedSyncs,
                partyPackageRequirements = partyPackageRequirements,
              )
            )
            .toFutureUS(identity)
      } yield perSynchronizerPreferenceSet

    private def applyRootPackageNamesRestriction(
        perSynchronizerCandidates: Map[PhysicalSynchronizerId, MapView[LfPackageName, Candidate[
          SortedPreferences
        ]]],
        rootPackageNames: Set[LfPackageName],
    )(implicit
        loggingContextWithTrace: LoggingContextWithTrace
    ): MapView[PhysicalSynchronizerId, Either[String, Set[LfPackageId]]] =
      perSynchronizerCandidates.view
        .mapValues {
          (packageNameCandidates: MapView[LfPackageName, Candidate[SortedPreferences]]) =>
            val unavailablePackageNames = rootPackageNames.diff(packageNameCandidates.keySet)
            // Discard a synchronizer if there are unavailable package-names pertaining to root nodes
            // This can happen if no one vetted any package from a specific package-name
            if (unavailablePackageNames.nonEmpty) {
              Left(
                show"Unable to find some package-names used in command root nodes: $unavailablePackageNames. Either these packages are not known on this participant or they are not vetted by the required informee participants. Please upload and vet the missing packages to proceed."
              )
            } else {
              packageNameCandidates.toSeq.foldM(Set.empty[LfPackageId]) {
                case (acc, (_, Right(pkgIdCandidates))) =>
                  Right(acc + pkgIdCandidates.last1.pkgId)
                case (_, (pkgName, Left(pkgNameDiscardReason))) if rootPackageNames(pkgName) =>
                  // Discard a synchronizer if there are package-names pertaining to root nodes that have no preferences
                  // This can happen if a package-name had vetted packages for some party, but it has been discarded due to some restrictions
                  Left(
                    show"Failed to select package-id for package-name '$pkgName' appearing in a command root node due to: $pkgNameDiscardReason"
                  )
                case (acc, (pkgName, Left(pkgNameDiscardReason))) =>
                  // If not a root-node package-name, just log the discard reason and continue
                  logDebug(
                    show"No vetted package selection possible for '$pkgName': $pkgNameDiscardReason"
                  )
                  Right(acc)
              }
            }
        }

    private def buildSelectionFailedError(
        prescribedSynchronizerIdO: Option[SynchronizerId],
        discardedSynchronizers: Seq[(PhysicalSynchronizerId, String)],
        partyPackageRequirements: Map[LfPartyId, Set[LfPackageName]],
    ): StatusRuntimeException = {
      val reason = show"Discarded synchronizers: ${discardedSynchronizers
          .map { case (sync, discardReason) => s"$sync: $discardReason" }
          .mkString("\n\t", "\n\t", "")}"

      prescribedSynchronizerIdO
        .map { prescribedSynchronizerId =>
          InvalidPrescribedSynchronizerId
            .Generic(prescribedSynchronizerId, reason)
            .asGrpcError
        }
        .getOrElse(
          CommandExecutionErrors.PackageSelectionFailed
            .Reject(
              s"No synchronizers satisfy the topology requirements for the submitted command: $reason"
            )
            .asGrpcError
        )
        .tap { _ =>
          logInfo(
            show"Party-package requirements used for the failed package selection: $partyPackageRequirements"
          )
        }
    }

    // TODO(#25385): Ideally the Engine already returns a specialized error instead
    //          of having the need to decide here whether the package-name was discarded or not
    private def refinePackageNotFoundError(
        errorCause: ErrorCause,
        locallyStoredPackageNames: Set[LfPackageName],
    )(implicit errorLoggingContext: ErrorLoggingContext): ErrorCause =
      // It can be that a missing or unresolved package name is due to package selection algorithm
      // removing it from the package-map provided to the engine due to topology constraints.
      // In these cases, report a dedicated error to the client to aid debugging.
      errorCause match {
        case ErrorCause.DamlLf(
              Package(Package.MissingPackage(Ref.PackageRef.Name(pkgName), context))
            ) if locallyStoredPackageNames(pkgName) =>
          ErrorCause.RoutingFailed(
            CommandExecutionErrors.PackageNameDiscardedDueToUnvettedPackages
              .Reject(pkgName, context)
          )

        case ErrorCause.DamlLf(Preprocessing(Preprocessing.UnresolvedPackageName(pkgName, context)))
            if locallyStoredPackageNames(pkgName) =>
          ErrorCause.RoutingFailed(
            CommandExecutionErrors.PackageNameDiscardedDueToUnvettedPackages
              .Reject(pkgName, context)
          )

        case other => other
      }

    private def logDebug(msg: => String)(implicit
        loggingContext: LoggingContextWithTrace
    ): Unit = logger.debug(s"Phase 1 [$tapsPassDescription]: $msg")(loggingContext.traceContext)

    private def logInfo(msg: => String)(implicit
        loggingContext: LoggingContextWithTrace
    ): Unit = logger.info(s"Phase 1 [$tapsPassDescription]: $msg")(loggingContext.traceContext)
  }

  private def orderUserSpecifiedPreferences(
      pkgIds: Set[LfPackageId],
      packageVersionMap: Map[LfPackageId, (LfPackageName, LfPackageVersion)],
  ): PackagesForName =
    pkgIds.view
      .flatMap(pkgId =>
        // TODO(#25385): Consider rejecting submissions where the resolution does not yield a package name for user-specified package-id
        pkgId.toPackageReference(packageVersionMap).map(pkgId -> _).orElse {
          logger.debug(
            show"Package $pkgId is not known. Discarding from user-specified package preferences in commands"
          )
          None
        }
      )
      .groupMap { case (_, PackageReference(_, _, pkgName)) => pkgName }(_._2)
      .view
      .mapValues(SortedSet.from[PackageReference])
      .toMap

  class TapsResult(
      val result: Either[TapsPassInterpretationFailed, TapsPassExecutionResult],
      previousPassLogDebug: String => Unit,
  ) {
    final def attemptNewPassOnRoutingFailed(
        nextPass: TapsPass[CommandInterpretationResult]
    ): FutureUnlessShutdown[TapsResult] =
      result.left
        .map { errCause =>
          previousPassLogDebug(
            s"$TapsDescription failed before synchronizer routing. Aborting submission. Error: $errCause"
          )
          this
        }
        .traverse {
          case TapsPassExecutionResult.RoutingFailed(interpretationResult, RoutingFailed(err)) =>
            previousPassLogDebug(
              s"Failed synchronizer routing: ${err.code.toMsg(cause = err.cause, correlationId = loggingContextWithTrace.traceContext.traceId, limit = None)}"
            )
            nextPass.execute(interpretationResult)
          case TapsPassExecutionResult.Succeeded(commandExecutionResult) =>
            previousPassLogDebug(
              s"$TapsDescription succeeded. Routing transaction for synchronization to ${commandExecutionResult.synchronizerRank.synchronizerId}"
            )
            FutureUnlessShutdown.pure(
              new TapsResult(
                Right(TapsPassExecutionResult.Succeeded(commandExecutionResult)),
                previousPassLogDebug,
              )
            )
        }
        .map(_.merge)

    def toSubmissionResult: Either[ErrorCause, CommandExecutionResult] =
      result.left.map(_.cause).flatMap {
        case TapsPassExecutionResult.RoutingFailed(_, routingFailed) =>
          Left(routingFailed)
        case TapsPassExecutionResult.Succeeded(commandExecutionResult) =>
          Right(commandExecutionResult)
      }
  }
}

object TapsCommandExecutionFactory {
  type PackagesForName =
    Map[LfPackageName, SortedSet[PackageReference] /* least preferred first */ ]
  private[execution] val TapsDescription = "Topology-aware package selection for command submission"
}

object TapsResult {
  // Command execution failed at the interpretation stage
  // and the submission should be rejected
  final case class TapsPassInterpretationFailed(cause: ErrorCause)

  // Models the outcomes of the first pass of the algorithm that can continue towards a successful command execution
  sealed trait TapsPassExecutionResult extends Product with Serializable

  object TapsPassExecutionResult {
    final case class RoutingFailed(
        interpretation: CommandInterpretationResult,
        cause: ErrorCause.RoutingFailed,
    ) extends TapsPassExecutionResult

    final case class Succeeded(commandExecutionResult: CommandExecutionResult)
        extends TapsPassExecutionResult
  }
}
