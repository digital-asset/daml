// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import cats.implicits.{catsSyntaxAlternativeSeparate, catsSyntaxParallelTraverse1, toFoldableOps}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.ledger.api.PackageReference.*
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
  PackageFilterRestriction,
  SortedPreferences,
}
import com.digitalasset.canton.platform.apiserver.execution.TopologyAwareCommandExecutor.{
  PackagesForName,
  Pass1ContinuationResult,
  Pass1InterpretationFailed,
}
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.apiserver.services.ErrorCause.RoutingFailed
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPackageVersion, LfPartyId, checked}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName, Party}
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.engine.Error.{Package, Preprocessing}
import com.digitalasset.daml.lf.transaction.SubmittedTransaction
import io.grpc.StatusRuntimeException

import scala.collection.MapView
import scala.collection.immutable.SortedSet
import scala.concurrent.ExecutionContext

// TODO(#25385): Consider introducing performance observability metrics
//               due to the high computational complexity of the algorithm
/** Command executor that performs the topology-aware package selection algorithm for command
  * interpretation.
  *
  * Note: The user-specified package preference set in `Commands.package_id_selection_preference` is
  * not honored if it conflicts with the preference set computed by this algorithm. It is only used
  * to fine-tune the selection.
  *
  * TODO(#25385): Add algorithm outline
  */
private[execution] class TopologyAwareCommandExecutor(
    syncService: SyncService,
    commandInterpreter: CommandInterpreter,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends NamedLogging
    with CommandExecutor {

  override def execute(
      commands: Commands,
      submissionSeed: Hash,
      routingSynchronizerState: RoutingSynchronizerState,
      forExternallySigned: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, ErrorCause, CommandExecutionResult] = {
    val submitterParty = commands.actAs.headOption.getOrElse(
      throw new IllegalArgumentException("act_as must be non-empty")
    )

    val packageMetadataSnapshot = syncService.getPackageMetadataSnapshot

    val pkgSelectionDesc = "topology-aware package selection command processing"

    val userSpecifiedPreference: PackagesForName =
      toOrderedPackagePreferences(
        pkgIds = commands.packagePreferenceSet,
        packageVersionMap = packageMetadataSnapshot.packageIdVersionMap,
        context = "user-specified package preferences in commands",
      )

    logDebug(s"Attempting pass 1 of $pkgSelectionDesc - using the submitter party")
    pass1(
      submitterParty = submitterParty,
      commands = commands,
      submissionSeed = submissionSeed,
      packageMetadataSnapshot = packageMetadataSnapshot,
      userSpecifiedPreferences = userSpecifiedPreference,
      forExternallySigned = forExternallySigned,
      routingSynchronizerState = routingSynchronizerState,
    ).leftMap(_.cause)
      .leftSemiflatTap { errCause =>
        logDebug(
          s"Pass 1 of $pkgSelectionDesc failed before routing. Aborting submission. Error: $errCause"
        )
        FutureUnlessShutdown.unit
      }
      .flatMap[ErrorCause, CommandExecutionResult] {
        case Pass1ContinuationResult.Pass1RoutingFailed(interpretationResult, RoutingFailed(err)) =>
          // TODO(#25385): (Optimization) Do not attempt pass 2 on every error
          logDebug(s"Pass 1 of $pkgSelectionDesc failed synchronizer routing: ${err.code
              .toMsg(cause = err.cause, correlationId = loggingContext.traceContext.traceId, limit = None)}")
          logDebug(s"Attempting pass 2 of $pkgSelectionDesc - using the draft transaction")
          pass2(
            commands = commands,
            userSpecifiedPreference = userSpecifiedPreference,
            submissionSeed = submissionSeed,
            packageMetadataSnapshot = packageMetadataSnapshot,
            interpretationResultFromPass1 = interpretationResult,
            forExternallySigned = forExternallySigned,
            routingSynchronizerState = routingSynchronizerState,
          )
        case Pass1ContinuationResult.Pass1Succeeded(commandExecutionResult) =>
          EitherT.rightT(commandExecutionResult)
      }
      .semiflatTap { commandExecutionResult =>
        logDebug(
          s"${pkgSelectionDesc.capitalize} succeeded. Routing transaction for synchronization to ${commandExecutionResult.synchronizerRank.synchronizerId}"
        )
        FutureUnlessShutdown.unit
      }
  }

  private def pass1(
      submitterParty: Party,
      commands: Commands,
      submissionSeed: Hash,
      packageMetadataSnapshot: PackageMetadata,
      userSpecifiedPreferences: PackagesForName,
      forExternallySigned: Boolean,
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, Pass1InterpretationFailed, Pass1ContinuationResult] =
    for {
      packagePreferenceSetPass1 <- EitherT
        .right(
          computePackagePreferenceSetPass1(
            vettingValidityTimestamp = commands.submittedAt,
            userSpecifiedPreferences = userSpecifiedPreferences,
            submitterParty = submitterParty,
            packageMetadataSnapshot = packageMetadataSnapshot,
            prescribedSynchronizerIdO = commands.synchronizerId,
            forExternallySigned = forExternallySigned,
            routingSynchronizerState = routingSynchronizerState,
          )
        )
      _ = logTrace(s"Using package preference set for pass 1: $packagePreferenceSetPass1")
      commandsWithPackageSelectionForPass1 =
        commands.copy(packagePreferenceSet = packagePreferenceSetPass1)
      commandInterpretationResult <- EitherT(
        commandInterpreter.interpret(commandsWithPackageSelectionForPass1, submissionSeed)
      ).leftMap(refinePackageNotFoundError(_, packageMetadataSnapshot.packageNameMap.keySet))
        .leftMap(Pass1InterpretationFailed(_))
      pass1ContinuationResult <- syncService
        .selectRoutingSynchronizer(
          commandInterpretationResult.submitterInfo,
          commandInterpretationResult.transaction,
          commandInterpretationResult.transactionMeta,
          commandInterpretationResult.processedDisclosedContracts.map(_.contractId).toList,
          commandInterpretationResult.optSynchronizerId,
          transactionUsedForExternalSigning = forExternallySigned,
          routingSynchronizerState = routingSynchronizerState,
        )
        .map[Pass1ContinuationResult] { synchronizerRank =>
          // Pass 1 succeeded - return the command execution result
          Pass1ContinuationResult.Pass1Succeeded(
            commandInterpretationResult.toCommandExecutionResult(
              synchronizerRank,
              routingSynchronizerState,
            )
          )
        }
        .leftFlatMap[Pass1ContinuationResult, Pass1InterpretationFailed](err =>
          // Pass 1 failed at routing stage - return the command interpretation result
          // that should be used for retry-ing the execution in the second pass
          EitherT.rightT[FutureUnlessShutdown, Pass1InterpretationFailed](
            Pass1ContinuationResult.Pass1RoutingFailed(
              commandInterpretationResult,
              ErrorCause.RoutingFailed(err),
            )
          )
        )
    } yield pass1ContinuationResult

  private def pass2(
      commands: Commands,
      userSpecifiedPreference: PackagesForName,
      submissionSeed: Hash,
      packageMetadataSnapshot: PackageMetadata,
      interpretationResultFromPass1: CommandInterpretationResult,
      forExternallySigned: Boolean,
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[
    FutureUnlessShutdown,
    ErrorCause,
    CommandExecutionResult,
  ] =
    for {
      preselectedSynchronizerAndPreferenceSet <- EitherT.right[ErrorCause](
        computePackagePreferenceSetPass2(
          vettingValidityTimestamp = commands.submittedAt,
          packageMetadataSnapshot = packageMetadataSnapshot,
          interpretationResultFromPass1 = interpretationResultFromPass1,
          userSpecifiedPreferences = userSpecifiedPreference,
          forExternallySigned = forExternallySigned,
          routingSynchronizerState = routingSynchronizerState,
        )
      )
      (preselectedSynchronizerId, packagePreferenceSetPass2) =
        preselectedSynchronizerAndPreferenceSet
      _ = logTrace(s"Using package preference set for pass 2: $packagePreferenceSetPass2")
      interpretationResult <- EitherT(
        commandInterpreter.interpret(
          commands.copy(packagePreferenceSet = packagePreferenceSetPass2),
          submissionSeed,
        )
      ).leftMap(refinePackageNotFoundError(_, packageMetadataSnapshot.packageNameMap.keySet))
      synchronizerRank <-
        syncService
          .selectRoutingSynchronizer(
            submitterInfo = interpretationResult.submitterInfo,
            optSynchronizerId = interpretationResult.optSynchronizerId,
            transactionMeta = interpretationResult.transactionMeta,
            transaction = interpretationResult.transaction,
            disclosedContractIds =
              interpretationResult.processedDisclosedContracts.map(_.contractId).toList,
            transactionUsedForExternalSigning = forExternallySigned,
            routingSynchronizerState = routingSynchronizerState,
          )
          .leftMap(ErrorCause.RoutingFailed(_): ErrorCause)
          .semiflatTap {
            case chosenSynchronizerRank
                if chosenSynchronizerRank.synchronizerId != preselectedSynchronizerId =>
              FutureUnlessShutdown.pure(
                logger.info(
                  s"Preselected synchronizer-id ($preselectedSynchronizerId) differs from the selected synchronizer-id (${chosenSynchronizerRank.synchronizerId})"
                )
              )
            case _ => FutureUnlessShutdown.unit
          }
    } yield interpretationResult.toCommandExecutionResult(
      synchronizerRank,
      routingSynchronizerState,
    )

  private def computePackagePreferenceSetPass1(
      vettingValidityTimestamp: Time.Timestamp,
      userSpecifiedPreferences: PackagesForName,
      submitterParty: Party,
      packageMetadataSnapshot: PackageMetadata,
      prescribedSynchronizerIdO: Option[SynchronizerId],
      forExternallySigned: Boolean,
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[LfPackageId]] =
    for {
      packageMap: Map[PhysicalSynchronizerId, Map[LfPartyId, Set[PackageId]]] <- syncService
        .computePartyVettingMap(
          submitters = Option.unless(forExternallySigned)(submitterParty).iterator.toSet,
          informees = Set(submitterParty),
          vettingValidityTimestamp = CantonTimestamp(vettingValidityTimestamp),
          prescribedSynchronizer = prescribedSynchronizerIdO,
          routingSynchronizerState = routingSynchronizerState,
        )

      vettedPackagesForTheSubmitter = packageMap.view.mapValues(
        _.getOrElse(
          submitterParty,
          throw new RuntimeException(
            "TODO(#25385) Graceful handling: the package map should only contain vetted packages for the submitter party"
          ),
        )
      )

      // Union the submitter's vetted package ids across synchronizers
      // based on the following assumptions:
      //  - For each package name, the party's hosting participants have vetted
      //    the same packages on all synchronizers compatible with the application (homogeneous vetting).
      //  - If the homogeneous vetting assumption does not hold,
      //    synchronizers with differing vetting states will be implicitly discarded
      //    later by the synchronizer routing due to failing vetting checks.
      allPossiblePackageIdsOfTheSubmitter = vettedPackagesForTheSubmitter.values.flatten.toSet
      topologyAwarePreferenceMap: PackagesForName =
        toOrderedPackagePreferences(
          pkgIds = allPossiblePackageIdsOfTheSubmitter,
          packageVersionMap = packageMetadataSnapshot.packageIdVersionMap,
          context = show"vetted packages of the submitter party $submitterParty",
        )

      packagePreferenceSet <- topologyAwarePreferenceMap.toList
        .parTraverse { case (pkgName, topologyBasedPreferenceSetForPkgName) =>
          FutureUnlessShutdown.fromTry(
            mergeWithUserBasedPreferenceAndPickHighest(
              userSpecifiedPreferences,
              pkgName,
              topologyBasedPreferenceSetForPkgName,
            ).toTry
          )
        }
        .map(_.toSet)
    } yield packagePreferenceSet

  private def mergeWithUserBasedPreferenceAndPickHighest(
      userSpecifiedPreferenceMap: PackagesForName,
      pkgName: LfPackageName,
      topologyBasedPreferenceSetForPkgName: SortedSet[PackageReference],
  )(implicit traceContext: TraceContext): Either[StatusRuntimeException, LfPackageId] = {
    val preferredTopologyBasedPackage = checked(
      topologyBasedPreferenceSetForPkgName.lastOption
        .getOrElse(
          throw new RuntimeException(
            "Topology based preference set should not be empty for a package name"
          )
        )
    )
    userSpecifiedPreferenceMap
      .get(pkgName)
      .map(userPreferenceForPkgName =>
        userPreferenceForPkgName
          .intersect(topologyBasedPreferenceSetForPkgName)
          .lastOption
          .toRight(
            CommandExecutionErrors.UserPackagePreferenceNotVetted
              .Reject(packageName = pkgName)
              .asGrpcError
          )
      )
      .getOrElse(Right(preferredTopologyBasedPackage))
      .map(_.pkgId)
  }

  private def computePackagePreferenceSetPass2(
      vettingValidityTimestamp: Time.Timestamp,
      packageMetadataSnapshot: PackageMetadata,
      interpretationResultFromPass1: CommandInterpretationResult,
      userSpecifiedPreferences: PackagesForName,
      forExternallySigned: Boolean,
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): FutureUnlessShutdown[(PhysicalSynchronizerId, Set[LfPackageId])] = {
    val draftTransaction = interpretationResultFromPass1.transaction
    val packageIndexSnapshot: Map[PackageId, (PackageName, canton.LfPackageVersion)] =
      packageMetadataSnapshot.packageIdVersionMap

    val draftPartyPackages: Map[LfPartyId, Set[LfPackageName]] =
      // This gives us all the party -> package-names, even the ones that were statically linked.
      Blinding
        .partyPackages(interpretationResultFromPass1.transaction)
        .map { case (party, pkgIds) =>
          party -> pkgIds.map(
            // It is fine to use unsafe here since the package must have been indexed on the participant
            // if it appeared in the draft transaction
            _.unsafeToPackageReference(packageIndexSnapshot).packageName
          )
        }

    val rootPackageNames = draftTransaction.rootNodes.iterator
      .flatMap(_.packageIds)
      // It is fine to use unsafe here since the package must have been indexed on the participant
      // if it appeared in the draft transaction
      .map(_.unsafeToPackageReference(packageIndexSnapshot))
      .map(_.packageName)
      .toSet

    for {
      synchronizersPartiesVettingState: Map[
        PhysicalSynchronizerId,
        Map[LfPartyId, Set[PackageId]],
      ] <-
        syncService
          .computePartyVettingMap(
            submitters = Option
              .unless(forExternallySigned)(authorizersOf(draftTransaction))
              .getOrElse(Set.empty),
            informees = draftTransaction.informees,
            vettingValidityTimestamp = CantonTimestamp(vettingValidityTimestamp),
            prescribedSynchronizer = interpretationResultFromPass1.optSynchronizerId,
            routingSynchronizerState = routingSynchronizerState,
          )

      perSynchronizerPreferenceSet <- FutureUnlessShutdown.fromTry(
        computePerSynchronizerPackagePreferenceSet(
          rootPackageNames = rootPackageNames,
          prescribedSynchronizerIdO = interpretationResultFromPass1.optSynchronizerId,
          synchronizersPartiesVettingState = synchronizersPartiesVettingState,
          packageMetadataSnapshot = packageMetadataSnapshot,
          draftPartyPackages = draftPartyPackages,
          userSpecifiedPreferenceMap = userSpecifiedPreferences,
        ).toTry
      )

      // TODO(#25385): Sort the synchronizers by the version of each package-name preference
      //               taken in the order of execution from the draft transaction
      synchronizerId <-
        syncService
          .computeHighestRankedSynchronizerFromAdmissible(
            submitterInfo = interpretationResultFromPass1.submitterInfo,
            transaction = draftTransaction,
            transactionMeta = interpretationResultFromPass1.transactionMeta,
            admissibleSynchronizers = perSynchronizerPreferenceSet.keySet,
            disclosedContractIds = interpretationResultFromPass1.processedDisclosedContracts
              .map(_.contractId)
              .toList,
            routingSynchronizerState = routingSynchronizerState,
          )
          .leftSemiflatMap(err => FutureUnlessShutdown.failed(err.asGrpcError))
          .merge
      packagePreferenceSet = checked(perSynchronizerPreferenceSet(synchronizerId))
    } yield synchronizerId -> packagePreferenceSet
  }

  private def authorizersOf(transaction: SubmittedTransaction): Set[Party] =
    transaction.rootNodes.iterator.flatMap(_.requiredAuthorizers).toSet

  private def computePerSynchronizerPackagePreferenceSet(
      rootPackageNames: Set[LfPackageName],
      prescribedSynchronizerIdO: Option[SynchronizerId],
      synchronizersPartiesVettingState: Map[PhysicalSynchronizerId, Map[LfPartyId, Set[PackageId]]],
      packageMetadataSnapshot: PackageMetadata,
      draftPartyPackages: Map[LfPartyId, Set[LfPackageName]],
      userSpecifiedPreferenceMap: PackagesForName,
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Either[StatusRuntimeException, NonEmpty[Map[PhysicalSynchronizerId, Set[LfPackageId]]]] = {
    logTrace(
      s"Computing per-synchronizer package preference sets using the draft transaction's party-packages ($draftPartyPackages)"
    )
    val (discardedSyncs, availableSyncs) = PackagePreferenceBackend
      .computePerSynchronizerPackageCandidates(
        synchronizersPartiesVettingState = synchronizersPartiesVettingState,
        packageMetadataSnapshot = packageMetadataSnapshot,
        packageFilter = PackageFilterRestriction(
          supportedPackagesPerPackagename =
            userSpecifiedPreferenceMap.view.mapValues(_.map(_.pkgId).toSet).toMap,
          restrictionDescription = "Commands.package_id_selection_preference",
        ),
        requirements = draftPartyPackages,
        logger = logger,
      )
      .view
      .mapValues { (packageNameCandidates: MapView[LfPackageName, Candidate[SortedPreferences]]) =>
        val unavailablePackageNames = rootPackageNames.diff(packageNameCandidates.keySet)
        // Discard a synchronizer if there are unavailable package-names pertaining to root nodes from the draft transaction
        // This can happen if no one vetted any package from a specific package-name
        if (unavailablePackageNames.nonEmpty) {
          Left(
            show"Package-names appeared in the draft transaction root nodes that have no vetted candidates: $unavailablePackageNames"
          )
        } else {
          packageNameCandidates.toSeq.foldM(Set.empty[LfPackageId]) {
            case (acc, (_pkgName, Right(pkgIdCandidates))) =>
              Right(acc + pkgIdCandidates.last1.pkgId)
            case (acc, (pkgName, Left(pkgNameDiscardReason))) if rootPackageNames(pkgName) =>
              // Discard a synchronizer if there are package-names pertaining to root nodes from the draft transaction that have no preferences
              // This can happen if a package-name had vetted packages for some party, but it has been discarded due to some restrictions
              Left(
                show"Package-name '$pkgName' appearing in a draft transaction root node has been discarded: $pkgNameDiscardReason"
              )
            case (acc, (pkgName, Left(pkgNameDiscardReason))) =>
              logger.debug(
                show"Dropped requirement to pre-select vetted package for '$pkgName', as there is no candidate due to: $pkgNameDiscardReason"
              )
              Right(acc)
          }
        }
      }
      .view
      .map { case (sync, candidates) => candidates.map(sync -> _).left.map(sync -> _) }
      .toSeq
      .separate

    NonEmpty
      .from(availableSyncs.toMap)
      .toRight(
        buildSelectionFailedError(
          prescribedSynchronizerIdO,
          show"Discarded synchronizers:\n${discardedSyncs
              .map { case (sync, reason) => s"$sync: $reason" }}",
        )
      )
  }

  private def buildSelectionFailedError(
      prescribedSynchronizerIdO: Option[SynchronizerId],
      reason: String,
  )(implicit
      tc: TraceContext
  ): StatusRuntimeException =
    prescribedSynchronizerIdO
      .map { prescribedSynchronizerId =>
        InvalidPrescribedSynchronizerId
          .Generic(prescribedSynchronizerId, reason)
          .asGrpcError
      }
      .getOrElse(
        CommandExecutionErrors.PackageSelectionFailed
          .Reject(s"No synchronizers satisfy the draft transaction topology requirements: $reason")
          .asGrpcError
      )

  private def toOrderedPackagePreferences(
      pkgIds: Set[LfPackageId],
      packageVersionMap: Map[LfPackageId, (LfPackageName, LfPackageVersion)],
      context: String,
  )(implicit traceContext: TraceContext): PackagesForName =
    pkgIds.view
      .flatMap(pkgId =>
        // TODO(#25385): Reject submissions where the resolution does not yield a package name
        //                     for non-utility packages
        pkgId.toPackageReference(packageVersionMap).map(pkgId -> _).orElse {
          logger.debug(show"Package $pkgId is not known. Discarding from $context")
          None
        }
      )
      .groupMap { case (_pkgId, PackageReference(_, _, pkgName)) => pkgName }(_._2)
      .view
      .mapValues(SortedSet.from[PackageReference])
      .toMap

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
      case ErrorCause.DamlLf(Package(Package.MissingPackage(Ref.PackageRef.Name(pkgName), context)))
          if locallyStoredPackageNames(pkgName) =>
        ErrorCause.RoutingFailed(
          CommandExecutionErrors.PackageNameDiscardedDueToUnvettedPackages.Reject(pkgName, context)
        )

      case ErrorCause.DamlLf(Preprocessing(Preprocessing.UnresolvedPackageName(pkgName, context)))
          if locallyStoredPackageNames(pkgName) =>
        ErrorCause.RoutingFailed(
          CommandExecutionErrors.PackageNameDiscardedDueToUnvettedPackages.Reject(pkgName, context)
        )

      case other => other
    }

  private def logDebug(msg: => String)(implicit
      loggingContext: LoggingContextWithTrace
  ): Unit = logger.debug(s"Phase 1: $msg")

  private def logTrace(msg: => String)(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Unit = logger.trace(s"Phase 1: $msg")
}

private[execution] object TopologyAwareCommandExecutor {
  private type PackagesForName =
    Map[LfPackageName, SortedSet[PackageReference] /* least preferred first */ ]
  // Command execution failed at the interpretation stage
  // and the submission should be rejected
  final case class Pass1InterpretationFailed(cause: ErrorCause)

  // Models the outcomes of the first pass of the algorithm that can continue towards a successful command execution
  private sealed trait Pass1ContinuationResult extends Product with Serializable

  private object Pass1ContinuationResult {
    final case class Pass1RoutingFailed(
        interpretation: CommandInterpretationResult,
        cause: ErrorCause.RoutingFailed,
    ) extends Pass1ContinuationResult

    final case class Pass1Succeeded(commandExecutionResult: CommandExecutionResult)
        extends Pass1ContinuationResult
  }
}
