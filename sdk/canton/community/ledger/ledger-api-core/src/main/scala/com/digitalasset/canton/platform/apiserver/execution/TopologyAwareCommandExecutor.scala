// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import com.digitalasset.canton.ledger.api.Commands
import com.digitalasset.canton.ledger.api.PackageReference.*
import com.digitalasset.canton.ledger.participant.state.{RoutingSynchronizerState, SyncService}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.{LfPackageRef, checked}
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{FullReference, PackageName, Party}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.SubmittedTransaction

import scala.concurrent.ExecutionContext

// TODO(#25385): Consider introducing performance observability metrics
//               due to the high computational complexity of the algorithm
/** Command executor that uses the '''topology-aware package selection''' algorithm for computing
  * the package preference set to-be-used by the Daml Engine in command interpretation.
  *
  * =Topology-Aware Package Selection (TAPS)=
  *
  * The topology-aware package selection (abbreviated further as '''TAPS''') algorithm is a
  * heuristic that computes a package-name-to-package-ID map to be used by the Daml Engine in
  * command interpretation for resolving the packages that contracts must be interpreted with in for
  * the purpose of implementing up/downgrading.
  *
  * TAPS uses approximations of topology requirements as input, because the exact requirements (e.g.
  * which informees, on which synchronizers, require which packages) can only be precisely derived
  * from the Daml transaction resulted from command interpretation.
  *
  * The situations in which the Engine needs to resolve package names to package IDs during command
  * interpretation for up/downgrading are:
  *
  *   - A top-level command is submitted for a contract with a template-id specified using the
  *     package-name reference format (see [[com.daml.ledger.api.v2.value.Identifier]])
  *
  *   - An exercise-by-interface or fetch-by-interface in an action node in the interpreted
  *     transaction
  *
  * The algorithm depends on the following key definitions:
  *
  *   - '''Party-level vetted package''': A package that is vetted by every participant hosting a
  *     specific party. Also referred to as a consistently-vetted package for a party.
  *
  *   - '''Party interest in a package-name''': A party is interested in a package-name if it has
  *     vetted any package ID pertaining to that package name.
  *
  *   - '''Commonly-vetted package''': A package that is vetted by all parties interested in the
  *     package's package name and whose direct and transitive dependencies are commonly-vetted.
  *
  * Since each package ID has a unique package name, the package-name-to-package ID resolution can
  * simply be represented by its image: a set of package IDs whose package names are all different.
  * We refer to this set as the '''package preference set'''
  *
  * TAPS chooses the synchronizer during package preference set computation, as package vetting and
  * party hosting are topology information tied to specific synchronizers. While the synchronizer
  * choice remains hidden from the Engine, the provided package preference set ensures compliance
  * with topology constraints of the selected synchronizer.
  *
  * As part of command execution, there are two TAPS attempts:
  *   - A first - approximation - pass that uses information derived only from the submitted command
  *     for computing the package preference set.
  *   - A second - refinement - pass that uses the information derived from the interpreted
  *     transaction obtained from the first pass for computing a more accurate package preference
  *     set.
  *
  * Each TAPS pass generally performs the following steps:
  *
  *   1. '''Define Input Topology Requirements''' - find the topology constraints that the
  *      interpreted Daml transaction should satisfy when evaluating submission candidate
  *      synchronizers:
  *      a. Expected transaction submitters: which parties should have submission rights on the
  *         preparing participant
  *      a. Expected transaction informees: which parties should have at least observation rights on
  *         some participant
  *      a. Root-node package-names vetting requirements: the package-names of the command's root
  *         nodes for which there must exist commonly-vetted package IDs for the expected informees
  *         of these nodes.
  *
  *   1. '''Compute Per-Synchronizer Package Preference Set''': Based on the Input Topology
  *      transaction informees requirement, a package preference set is computed for each
  *      synchronizer connected to the preparing participant. This set contains the
  *      highest-versioned ''commonly-vetted'' package ID for each package name the expected
  *      transaction's informees are interested in. '''Note''': Only synchronizers with valid
  *      package candidates in the package preference set for all the command's root package-names
  *      are admissible and used for further TAPS processing.
  *
  *   1. '''Process Package Preference Set''': The per-synchronizer package preference set is then
  *      processed into the package preference set used for interpretation (which differs between
  *      Pass 1 and Pass 2).
  *
  *   1. '''Interpret Commands''': The Daml Engine interprets the submitted commands using the
  *      computed package preference set (see [[com.digitalasset.daml.lf.engine.Engine.submit]]
  *
  *   1. '''Route Transaction''': If interpretation is successful, synchronizer routing searches for
  *      a suitable synchronizer that satisfies the interpreted transaction's topology constraints
  *      (see
  *      [[com.digitalasset.canton.ledger.participant.state.SyncService.selectRoutingSynchronizer]]).
  *      If found, the transaction is routed for protocol synchronization.
  *
  * The two passes of the algorithm differ as follows:
  *
  * ==Pass 1==
  *
  * The input topology requirements are derived from the submitter party, which is conventionally
  * the first party of `Commands.act_as`. This party is considered the submitter and sole informee
  * of the transaction. The package-name requirements are derived from the package-names of all of
  * the submitted command’s root nodes, leading to an input topology requirement modelled as
  * submitter-party -> Set[command_root_nodes_packages]. The resulting per-synchronizer preference
  * set is merged into a single package preference set by selecting the highest-versioned package
  * across all admissible synchronizers for the submitter's vetted package names. If synchronizer
  * routing in this pass doesn't yield a valid synchronizer, the algorithm proceeds to Pass 2.
  *
  * ==Pass 2==
  *
  * In this pass, the topology requirements are derived from the Daml transaction obtained during
  * the command interpretation resulting from the first pass, referred to below as the '''draft
  * transaction'''. These new requirements stipulate that every informee of the draft transaction
  * must have vetted a package ID corresponding to each package name used in the transaction. The
  * root-level package names are the same as in the first pass. The package preference set for
  * interpretation in Pass 2 is derived from the per-synchronizer package preference sets by
  * selecting the one associated with the highest-ranked admissible synchronizer (see
  * [[com.digitalasset.canton.ledger.participant.state.SyncService.computeHighestRankedSynchronizerFromAdmissible]]).
  * If the resulting Daml transaction after interpretation in Pass 2 cannot be routed to a valid
  * synchronizer, command submission fails.
  *
  * '''Note''': When `Commands.package_id_selection_preference` is specified, it acts as a
  * restriction in the package preference set computation for both passes. If this restriction
  * cannot be honored, command submission fails.
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
    val packageMetadataSnapshot = syncService.getPackageMetadataSnapshot

    val packageIndex = packageMetadataSnapshot.packageIdVersionMap

    val rootLevelPackageNames = apiCommandsRootPackageNames(commands)

    val tapsExecutionFactory = new TapsCommandExecutionFactory(
      commands = commands,
      commandInterpreter = commandInterpreter,
      forExternallySigned = forExternallySigned,
      loggerFactory = loggerFactory,
      packageMetadataSnapshot = packageMetadataSnapshot,
      rootLevelPackageNames = rootLevelPackageNames,
      routingSynchronizerState = routingSynchronizerState,
      submissionSeed = submissionSeed,
      syncService = syncService,
    )

    val tapsPass1: tapsExecutionFactory.TapsPass[Commands] = {
      val submitterParty =
        commands.actAs.headOption.getOrElse(sys.error("act_as must be non-empty"))
      new tapsExecutionFactory.TapsPass[Commands](
        tapsPassDescription = "1st TAPS pass",
        computeRequiredSubmitters = _ => Set(submitterParty),
        computePartyPackageRequirements = _ => Map(submitterParty -> rootLevelPackageNames),
        computePackagePreferenceSet = (_, perSynchronizerPreferenceSet) =>
          FutureUnlessShutdown.pure(
            perSynchronizerPreferenceSet.values.flatten
              .map(_.unsafeToPackageReference(packageIndex))
              .groupBy(_.packageName)
              .view
              .mapValues(
                _.maxOption.getOrElse(sys.error("Unexpected empty references set after groupBy"))
              )
              .values
              .map(_.pkgId)
              .toSet
          ),
      )
    }

    val tapsPass2: tapsExecutionFactory.TapsPass[CommandInterpretationResult] =
      new tapsExecutionFactory.TapsPass[CommandInterpretationResult](
        tapsPassDescription = "2nd TAPS pass",
        computeRequiredSubmitters = passInput =>
          rootNodesRequiredAuthorizers(passInput.transaction),
        computePartyPackageRequirements = passInput =>
          Blinding
            .partyPackages(passInput.transaction)
            .map { case (party, pkgIds) =>
              party -> pkgIds.map(
                // It is fine to use unsafe here since the package must have been indexed on the participant
                // if it appeared in the draft transaction
                _.unsafeToPackageReference(packageIndex).packageName
              )
            },
        computePackagePreferenceSet = (passInput, perSynchronizerPreferenceSet) =>
          syncService
            .computeHighestRankedSynchronizerFromAdmissible(
              submitterInfo = passInput.submitterInfo,
              transaction = passInput.transaction,
              transactionMeta = passInput.transactionMeta,
              admissibleSynchronizers = perSynchronizerPreferenceSet.keySet,
              disclosedContractIds = passInput.processedDisclosedContracts.map(_.contractId).toList,
              routingSynchronizerState = routingSynchronizerState,
            )
            .leftSemiflatMap(err => FutureUnlessShutdown.failed(err.asGrpcError))
            .merge
            .map { highestRankedSync =>
              checked(perSynchronizerPreferenceSet(highestRankedSync))
            },
      )

    EitherT {
      for {
        pass1Result <- tapsPass1.execute(commands)
        pass2Result <- pass1Result.attemptNewPassOnRoutingFailed(tapsPass2)
      } yield pass2Result.toSubmissionResult
    }
  }

  private def rootNodesRequiredAuthorizers(transaction: SubmittedTransaction): Set[Party] =
    transaction.rootNodes.iterator.flatMap(_.requiredAuthorizers).toSet

  // Note: This method also collect package-names for interfaces in case of exercise-by-interface commands
  //       even though interfaces are not upgradeable. However, this is fine
  //       since the package selection algorithm merely should still be able to find a commonly-vetted package
  //       for the package-names used, even though this preference is ignored by the Engine
  private def apiCommandsRootPackageNames(commands: Commands): Set[PackageName] =
    commands.commands.commands.iterator.collect {
      case ApiCommand.Create(FullReference(LfPackageRef.Name(pkgName), _), _) =>
        pkgName
      case ApiCommand.Exercise(FullReference(LfPackageRef.Name(pkgName), _), _, _, _) =>
        pkgName
      case ApiCommand.ExerciseByKey(FullReference(LfPackageRef.Name(pkgName), _), _, _, _) =>
        pkgName
      case ApiCommand.CreateAndExercise(FullReference(LfPackageRef.Name(pkgName), _), _, _, _) =>
        pkgName
    }.toSet
}
