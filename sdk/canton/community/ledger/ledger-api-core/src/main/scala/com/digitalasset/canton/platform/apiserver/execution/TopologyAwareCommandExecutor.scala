// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import cats.implicits.catsSyntaxParallelTraverse1
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.ContextualizedErrorLogger
import com.digitalasset.canton
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.Commands
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.participant.state.{RoutingSynchronizerState, SyncService}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.execution.TopologyAwareCommandExecutor.{
  OrderablePackageId,
  Pass1ContinuationResult,
  Pass1InterpretationFailed,
}
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPackageVersion, LfPartyId, checked}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName, Party}
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.engine.Error.{Package, Preprocessing}
import com.digitalasset.daml.lf.transaction.SubmittedTransaction

import scala.collection.View
import scala.collection.immutable.SortedSet
import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

/** Command executor that performs the topology-aware package selection algorithm for command
  * interpretation.
  *
  * Note: The user-specified package preference set in `Commands.package_id_selection_preference` is
  * not honored if it conflicts with the preference set computed by this algorithm. It is only used
  * to fine-tune the selection.
  *
  * TODO(#23334): Add algorithm outline
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

    def logDebug(msg: String): Unit = logger.debug(s"Phase 1: $msg")
    val pkgSelectionDesc = "topology-aware package selection command processing"

    val userSpecifiedPreferenceMap: Map[LfPackageName, SortedSet[OrderablePackageId]] =
      toOrderedPackagePreferenceMap(
        commands.packagePreferenceSet,
        packageMetadataSnapshot.packageIdVersionMap,
      )

    logDebug(s"Attempting pass 1 of $pkgSelectionDesc - using the submitter party")
    pass1(
      submitterParty = submitterParty,
      commands = commands,
      submissionSeed = submissionSeed,
      packageMetadataSnapshot = packageMetadataSnapshot,
      userSpecifiedPreferenceMap = userSpecifiedPreferenceMap,
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
        case Pass1ContinuationResult.Pass1RoutingFailed(interpretationResult, cause) =>
          // TODO(#23334): Do not attempt pass 2 on every error
          logDebug(s"Pass 1 of $pkgSelectionDesc failed synchronizer routing: $cause")
          logDebug(s"Attempting pass 2 of $pkgSelectionDesc - using the draft transaction")
          pass2(
            commands = commands,
            userSpecifiedPreferenceMap = userSpecifiedPreferenceMap,
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
      userSpecifiedPreferenceMap: Map[LfPackageName, SortedSet[OrderablePackageId]],
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
            userSpecifiedPreferenceMap = userSpecifiedPreferenceMap,
            submitterParty = submitterParty,
            packageMetadataSnapshot = packageMetadataSnapshot,
            prescribedSynchronizerIdO = commands.synchronizerId,
            forExternallySigned = forExternallySigned,
            routingSynchronizerState = routingSynchronizerState,
          )
        )
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
      userSpecifiedPreferenceMap: Map[LfPackageName, SortedSet[OrderablePackageId]],
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
          userSpecifiedPreferenceMap = userSpecifiedPreferenceMap,
          forExternallySigned = forExternallySigned,
          routingSynchronizerState = routingSynchronizerState,
        )
      )
      (preselectedSynchronizerId, packagePreferenceSet) = preselectedSynchronizerAndPreferenceSet
      interpretationResult <- EitherT(
        commandInterpreter.interpret(
          commands.copy(packagePreferenceSet = packagePreferenceSet),
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
      userSpecifiedPreferenceMap: Map[LfPackageName, SortedSet[OrderablePackageId]],
      submitterParty: Party,
      packageMetadataSnapshot: PackageMetadata,
      prescribedSynchronizerIdO: Option[SynchronizerId],
      forExternallySigned: Boolean,
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[LfPackageId]] =
    for {
      packageMap: Map[SynchronizerId, Map[LfPartyId, Set[PackageId]]] <- syncService.packageMapFor(
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
            "TODO(#23334) Graceful handling: the package map should only contain vetted packages for the submitter party"
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
      topologyAwarePreferenceMap: Map[LfPackageName, SortedSet[OrderablePackageId]] =
        toOrderedPackagePreferenceMap(
          allPossiblePackageIdsOfTheSubmitter,
          packageMetadataSnapshot.packageIdVersionMap,
        )

      packagePreferenceSet <- topologyAwarePreferenceMap.toList
        .parTraverse { case (pkgName, topologyBasedPreferenceSetForPkgName) =>
          mergeWithUserBasedPreferenceAndPickHighest(
            userSpecifiedPreferenceMap,
            pkgName,
            topologyBasedPreferenceSetForPkgName,
          )
            .pipe(FutureUnlessShutdown.pure)
        }
        .map(_.toSet)
    } yield packagePreferenceSet

  private def mergeWithUserBasedPreferenceAndPickHighest(
      userSpecifiedPreferenceMap: Map[LfPackageName, SortedSet[OrderablePackageId]],
      pkgName: LfPackageName,
      topologyBasedPreferenceSetForPkgName: SortedSet[OrderablePackageId],
  )(implicit traceContext: TraceContext): LfPackageId = {
    val preferredTopologyBasedPackage = checked(
      topologyBasedPreferenceSetForPkgName.headOption
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
          .headOption
          .getOrElse {
            logger.warn(
              s"User specified package preference set $userPreferenceForPkgName for package-name $pkgName could not be honored due to disjoint with the topology based preference set $topologyBasedPreferenceSetForPkgName"
            )
            preferredTopologyBasedPackage
          }
      )
      .getOrElse(preferredTopologyBasedPackage)
      .pkdId
  }

  private def computePackagePreferenceSetPass2(
      vettingValidityTimestamp: Time.Timestamp,
      packageMetadataSnapshot: PackageMetadata,
      interpretationResultFromPass1: CommandInterpretationResult,
      userSpecifiedPreferenceMap: Map[LfPackageName, SortedSet[OrderablePackageId]],
      forExternallySigned: Boolean,
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[(SynchronizerId, Set[LfPackageId])] = {
    val draftTransaction = interpretationResultFromPass1.transaction.transaction
    val knownPackagesMap: Map[PackageId, (PackageName, canton.LfPackageVersion)] =
      packageMetadataSnapshot.packageIdVersionMap

    val draftPartyPackages: Map[LfPartyId, Set[LfPackageName]] =
      // This gives us all the party -> package-names, even the ones that were statically linked.
      Blinding
        .partyPackages(interpretationResultFromPass1.transaction)
        .map { case (party, pkgIds) =>
          party -> toOrderedPackagePreferenceMap(pkgIds, knownPackagesMap).keySet
        }

    for {
      synchronizersPartiesVettingState: Map[SynchronizerId, Map[LfPartyId, Set[PackageId]]] <-
        syncService
          .packageMapFor(
            submitters = Option
              .unless(forExternallySigned)(authorizersOf(interpretationResultFromPass1.transaction))
              .getOrElse(Set.empty),
            informees = draftTransaction.informees,
            vettingValidityTimestamp = CantonTimestamp(vettingValidityTimestamp),
            prescribedSynchronizer = interpretationResultFromPass1.optSynchronizerId,
            routingSynchronizerState = routingSynchronizerState,
          )

      perSynchronizerPreferenceSet <- computePerSynchronizerPackagePreferenceSet(
        synchronizersPartiesVettingState = synchronizersPartiesVettingState,
        knownPackagesMap = knownPackagesMap,
        draftPartyPackages = draftPartyPackages,
        userSpecifiedPreferenceMap = userSpecifiedPreferenceMap,
      )

      synchronizerId <-
        syncService
          .computeHighestRankedSynchronizerFromAdmissible(
            submitterInfo = interpretationResultFromPass1.submitterInfo,
            transaction = interpretationResultFromPass1.transaction,
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
      synchronizersPartiesVettingState: Map[SynchronizerId, Map[LfPartyId, Set[PackageId]]],
      knownPackagesMap: Map[PackageId, (PackageName, canton.LfPackageVersion)],
      draftPartyPackages: Map[LfPartyId, Set[LfPackageName]],
      userSpecifiedPreferenceMap: Map[LfPackageName, SortedSet[OrderablePackageId]],
  )(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[NonEmpty[Map[SynchronizerId, Set[LfPackageId]]]] = {
    val syncsPartiesPackagePreferencesMap: Map[
      SynchronizerId,
      Map[LfPartyId, Map[LfPackageName, SortedSet[OrderablePackageId]]],
    ] =
      synchronizersPartiesVettingState.view.mapValues {
        _.view
          .mapValues(toOrderedPackagePreferenceMap(_, knownPackagesMap))
          .toMap
      }.toMap

    val syncsPartiesPackageMapAfterDraftIntersection: Map[
      SynchronizerId,
      Map[LfPartyId, Map[LfPackageName, SortedSet[OrderablePackageId]]],
    ] =
      syncsPartiesPackagePreferencesMap.filter {
        case (
              _syncId,
              partiesPackageMap: Map[LfPartyId, Map[LfPackageName, SortedSet[
                OrderablePackageId
              ]]],
            ) =>
          draftPartyPackages.forall { case (party, draftPackageNamesForParty) =>
            partiesPackageMap
              .get(party)
              .exists(packageMap => draftPackageNamesForParty.subsetOf(packageMap.keySet))
          }
      }

    val perSynchronizerPreferenceSet = syncsPartiesPackageMapAfterDraftIntersection.view
      .flatMap {
        case (
              syncId,
              partyPackagesTopology: Map[LfPartyId, Map[LfPackageName, SortedSet[
                OrderablePackageId
              ]]],
            ) =>
          // At this point we are reducing the party dimension by
          // intersecting all package-ids for a package-name of a party with the same for other parties
          val topologyAndDraftTransactionBasedPackageMap
              : Map[LfPackageName, SortedSet[OrderablePackageId]] =
            partyPackagesTopology.view.values.flatten.groupMapReduce(_._1)(_._2)(_ intersect _)

          // If an package preference set intersection for any package name for a synchronizer ultimately leads to 0,
          // the synchronizer should be discarded
          View(topologyAndDraftTransactionBasedPackageMap)
            .filterNot(_.exists(_._2.isEmpty))
            .map(syncId -> _)
      }
      .map { case (syncId, topologyAndDraftTransactionBasedPackageMap) =>
        syncId -> topologyAndDraftTransactionBasedPackageMap.view.map {
          case (pkgName, topologyBasedPreferenceSetForPkgName) =>
            mergeWithUserBasedPreferenceAndPickHighest(
              userSpecifiedPreferenceMap,
              pkgName,
              topologyBasedPreferenceSetForPkgName,
            )
        }.toSet
      }
      .toMap

    NonEmpty
      .from(perSynchronizerPreferenceSet)
      .map(FutureUnlessShutdown.pure)
      .getOrElse(
        FutureUnlessShutdown.failed(
          CommandExecutionErrors.PackageSelectionFailed
            .Reject(
              "No synchronizers satisfy the draft transaction topology requirements"
            )
            .asGrpcError
        )
      )
  }

  private def toOrderedPackagePreferenceMap(
      pkgIds: Set[LfPackageId],
      packageVersionMap: Map[LfPackageId, (LfPackageName, LfPackageVersion)],
  ): Map[LfPackageName, SortedSet[OrderablePackageId]] =
    pkgIds.view
      .flatMap(pkgId =>
        // The package metadata view does not store utility packages
        // TODO(#23334): Reject submissions where the resolution does not yield a package name
        //                     for non-utility packages
        packageVersionMap.get(pkgId).map(pkgId -> _)
      )
      .groupMap { case (_pkgId, (pkgName, _pkgVersion)) => pkgName } {
        case (pkgId, (_pkgName, pkgVersion)) => pkgId -> pkgVersion
      }
      .view
      .mapValues(s => SortedSet.from(s.map(e => OrderablePackageId(pkdId = e._1, version = e._2))))
      .toMap

  // TODO(#23334): Ideally the Engine already returns a specialized error instead
  //          of having the need to decide here whether the package-name was discarded or not
  private def refinePackageNotFoundError(
      errorCause: ErrorCause,
      locallyStoredPackageNames: Set[LfPackageName],
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): ErrorCause =
    // It can be that a missing or unresolved package name is due to package selection algorithm
    // removing it from the package-map provided to the engine due to topology constraints.
    // In these cases, report a dedicated error to the client to aid debugging.
    errorCause match {
      case ErrorCause.DamlLf(Package(Package.MissingPackage(Ref.PackageRef.Name(pkgName), context)))
          if locallyStoredPackageNames(pkgName) =>
        ErrorCause.RoutingFailed(
          CommandExecutionErrors.PackageNameDiscarded.Reject(pkgName, context)
        )

      case ErrorCause.DamlLf(Preprocessing(Preprocessing.UnresolvedPackageName(pkgName, context)))
          if locallyStoredPackageNames(pkgName) =>
        ErrorCause.RoutingFailed(
          CommandExecutionErrors.PackageNameDiscarded.Reject(pkgName, context)
        )

      case other => other
    }
}

private[execution] object TopologyAwareCommandExecutor {
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

  // Wrapper used for ordering package ids by version
  // Only relevant for sets of packages pertaining to the same package name
  private final case class OrderablePackageId(
      pkdId: LfPackageId,
      version: LfPackageVersion,
  )

  private object OrderablePackageId {
    implicit val ordering: Ordering[OrderablePackageId] =
      // Highest version first
      Ordering.by[OrderablePackageId, LfPackageVersion](_.version).reverse
  }
}
