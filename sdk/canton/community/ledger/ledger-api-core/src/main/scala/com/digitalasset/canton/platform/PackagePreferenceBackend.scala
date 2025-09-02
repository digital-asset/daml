// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import cats.implicits.{catsSyntaxAlternativeSeparate, toFoldableOps}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.PackageReference
import com.digitalasset.canton.ledger.api.PackageReference.*
import com.digitalasset.canton.ledger.api.validation.GetPreferredPackagesRequestValidator.PackageVettingRequirements
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound.PackageNamesNotFound
import com.digitalasset.canton.ledger.participant.state.SyncService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.platform.PackagePreferenceBackend.*
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPackageVersion, LfPartyId}
import com.digitalasset.daml.lf.language.Ast

import scala.collection.immutable.SortedSet
import scala.collection.{MapView, mutable}
import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

class PackagePreferenceBackend(
    clock: Clock,
    adminParty: LfPartyId,
    syncService: SyncService,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Computes the preferred package versions for the provided package vetting requirements.
    *
    * In detail, the method outputs the most preferred package for each package-name specified in
    * the package vetting requirements. For a package version to be eligible, it must satisfy the
    * following conditions:
    *
    *   1. It is vetted by all the parties specified in the value of its corresponding package
    *      vetting requirements map entry.
    *
    *   1. All its package dependencies are "commonly-vetted" by all the "interested" parties
    *
    * Note:
    *   - an interested party is a party that appears in the package vetting requirements
    *
    *   - a commonly-vetted package refers to a package-id that is vetted by all parties that have
    *     at least a package-id vetted pertaining to the dependency's package-name.
    *
    *   - for brevity, we refer here to a party vetting a package if all its hosting participants
    *     have vetted the package.
    *
    * @param packageVettingRequirements
    *   The package vetting requirements for which the package preferences should be computed.
    * @param packageFilter
    *   Filters which package IDs are eligible for consideration in the preference computation for
    *   each package-name specified in the provided requirements.
    * @param synchronizerId
    *   If provided, only this synchronizer's vetting topology state is considered in the
    *   computation. Otherwise, the highest package version from all the connected synchronizers is
    *   returned.
    * @param vettingValidAt
    *   If provided, used to compute the package vetting state at this timestamp.
    * @return
    *   if a solution exists, the best package preference coupled with the synchronizer id that it
    *   pertains to.
    */
  def getPreferredPackages(
      packageVettingRequirements: PackageVettingRequirements,
      packageFilter: PackageFilter,
      synchronizerId: Option[SynchronizerId],
      vettingValidAt: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, (Seq[PackageReference], PhysicalSynchronizerId)]] = {
    val routingSynchronizerState = syncService.getRoutingSynchronizerState
    val packageMetadataSnapshot = syncService.getPackageMetadataSnapshot

    for {
      _ <- ensurePackageNamesKnown(packageVettingRequirements, packageMetadataSnapshot)
      packageMapForRequest <- syncService.packageMapFor(
        submitters = Set.empty,
        informees = packageVettingRequirements.allParties,
        vettingValidityTimestamp = vettingValidAt.getOrElse(clock.now),
        prescribedSynchronizer = synchronizerId,
        routingSynchronizerState = routingSynchronizerState,
      )
      synchronizerCandidates = PackagePreferenceBackend
        .computePerSynchronizerPackageCandidates(
          synchronizersPartiesVettingState = packageMapForRequest,
          packageMetadataSnapshot = packageMetadataSnapshot,
          packageFilter = packageFilter,
          requirements = MapsUtil.transpose(packageVettingRequirements.value),
          logger = logger,
        )
        .view
        .mapValues((candidates: MapView[LfPackageName, Candidate[SortedPreferences]]) =>
          selectRequestedPackages(candidates, packageVettingRequirements.allPackageNames)
        )
        .toMap
    } yield findValidCandidate(synchronizerCandidates)
  }

  private def findValidCandidate(
      synchronizerCandidates: Map[PhysicalSynchronizerId, Candidate[Set[PackageReference]]]
  )(implicit
      traceContext: TraceContext
  ): Either[String, (Seq[PackageReference], PhysicalSynchronizerId)] = {
    val (discardedCandidates, validCandidates) = synchronizerCandidates.view
      .map { case (sync, candidateE) =>
        candidateE.left.map(sync -> _).map(sync -> _)
      }
      .toList
      .separate

    validCandidates
      .maxByOption(_._1)(
        // TODO(#25385): Order by the package version with the package precedence set by the order of the vetting requirements
        // Follow the pattern used for SynchronizerRank ordering,
        // where lexicographic order picks the most preferred synchronizer by id
        implicitly[Ordering[PhysicalSynchronizerId]].reverse
      )
      .map { case (syncId, packageRefs) =>
        // Valid candidate found
        // Log discarded candidates and return the package references and synchronizer id of the valid candidate
        if (discardedCandidates.nonEmpty) {
          logger.debug(show"Discarded synchronizers: $discardedCandidates")
        }
        packageRefs.toSeq -> syncId
      }
      .toRight(
        show"No synchronizer satisfies the vetting requirements. Discarded synchronizers: $discardedCandidates"
      )
  }

  def getPreferredPackageVersionForParticipant(
      packageName: PackageName,
      supportedPackageIds: Set[LfPackageId],
      supportedPackageIdsDescription: String,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Either[String, LfPackageId]] =
    getPreferredPackages(
      packageVettingRequirements = PackageVettingRequirements(
        value = Map(packageName -> Set(adminParty))
      ),
      packageFilter = PackageFilterRestriction(
        Map(packageName -> supportedPackageIds),
        supportedPackageIdsDescription,
      ),
      synchronizerId = None,
      vettingValidAt = Some(CantonTimestamp.MaxValue),
    )
      .map(_.map {
        case (Seq(pkgRef), _) => pkgRef.pkgId
        case (invalidSeq, syncId) =>
          throw new RuntimeException(
            s"Expected exactly one package reference for package name $packageName and $syncId, but got $invalidSeq. This is likely a programming error. Please contact support"
          )
      })

  private def ensurePackageNamesKnown(
      packageVettingRequirements: PackageVettingRequirements,
      packageMetadataSnapshot: PackageMetadata,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val requestPackageNames = packageVettingRequirements.allPackageNames
    val knownPackageNames = packageMetadataSnapshot.packageNameMap.keySet
    val unknownPackageNames = requestPackageNames.diff(knownPackageNames)

    if (unknownPackageNames.isEmpty) FutureUnlessShutdown.unit
    else
      FutureUnlessShutdown.failed(
        PackageNamesNotFound.Reject(unknownPackageNames).asGrpcError
      )
  }
}

object PackagePreferenceBackend {
  type SortedPreferences = NonEmpty[SortedSet[PackageReference] /* most preferred last */ ]
  private type PackageIndex = Map[LfPackageId, (LfPackageName, LfPackageVersion)]
  // A candidate refers to a value T that:
  //   - wraps the value in a Right if it is valid for package preferences computation OR
  //   - wraps the value's discarded reason in a Left
  type Candidate[T] = Either[String, T]

  sealed trait PackageFilter extends Product with Serializable with PrettyPrinting {
    def apply(packageName: LfPackageName, packageId: LfPackageId): Boolean
  }

  case object AllowAllPackageIds extends PackageFilter {
    def apply(packageName: LfPackageName, packageId: LfPackageId): Boolean = true

    override protected def pretty: Pretty[AllowAllPackageIds.this.type] =
      Pretty.prettyOfString(_ => "All package-ids supported")
  }

  final case class PackageFilterRestriction(
      supportedPackagesPerPackagename: Map[LfPackageName, Set[LfPackageId]],
      restrictionDescription: String,
  ) extends PackageFilter {
    def apply(packageName: LfPackageName, packageId: LfPackageId): Boolean =
      supportedPackagesPerPackagename.get(packageName).forall(_.contains(packageId))

    override protected def pretty: Pretty[PackageFilterRestriction] =
      Pretty.prettyOfString(_ => show"$restrictionDescription: $supportedPackagesPerPackagename")
  }

  def computePerSynchronizerPackageCandidates(
      synchronizersPartiesVettingState: Map[PhysicalSynchronizerId, Map[LfPartyId, Set[
        LfPackageId
      ]]],
      packageMetadataSnapshot: PackageMetadata,
      requirements: Map[LfPartyId, Set[LfPackageName]],
      packageFilter: PackageFilter,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Map[PhysicalSynchronizerId, MapView[LfPackageName, Candidate[SortedPreferences]]] = {
    val packageIndex = packageMetadataSnapshot.packageIdVersionMap
    synchronizersPartiesVettingState.view
      .mapValues(
        _.view
          // Resolve to full package references
          .mapValues(resolveAndOrderPackageReferences(_, packageIndex, logger))
          .pipe(
            (candidates: MapView[LfPartyId, Map[LfPackageName, Candidate[SortedPreferences]]]) =>
              // Decide candidates for which there is no vetted package satisfying a vetting requirement (party <-> package-name)
              considerPartyPackageNameRequirements(candidates, requirements)
          )
          .view
          .pipe {
            (candidates: MapView[LfPartyId, Map[LfPackageName, Candidate[SortedPreferences]]]) =>
              // At this point we are reducing the party dimension by
              // intersecting all package-ids for a package-name of a party with the same for other parties.
              computePartyPackageCandidatesIntersection(candidates)
          }
          // Preserve only the candidate package-ids that are vetted and all their dependencies are vetted
          .pipe(preserveDeeplyVetted(packageMetadataSnapshot, _))
          // Apply package-id filter restriction to the candidate package-ids
          // Note: the filter can discard packages that are dependencies of other candidates
          .pipe(filterPackages(packageFilter, _))
      )
      .toMap
  }

  // Preserve for each package-name all the package-ids that are vetted and all their dependencies are vetted
  private def preserveDeeplyVetted(
      packageMetadataSnapshot: PackageMetadata,
      candidatesForPackageName: Map[LfPackageName, Candidate[SortedPreferences]],
  ): MapView[LfPackageName, Candidate[SortedPreferences]] = {
    val packageIndex = packageMetadataSnapshot.packageIdVersionMap
    val dependencyGraph: Map[PackageId, Set[PackageId]] =
      packageMetadataSnapshot.packages.view.mapValues(_.directDeps).toMap

    val allVettedPackages = candidatesForPackageName.view.values
      .collect { case Right(vettedCandidates) =>
        vettedCandidates.view.map(_.pkgId)
      }
      .flatten
      .toSet

    val allDepsVettedForCached: mutable.Map[LfPackageId, Either[LfPackageId, Unit]] =
      mutable.Map.empty

    // Note: Keeping it simple without tailrec since the dependency graph depth should be limited
    def isDeeplyVetted(pkgId: LfPackageId): Either[LfPackageId, Unit] = {
      val pkg = packageMetadataSnapshot.packages.getOrElse(
        pkgId,
        throw new NoSuchElementException(
          s"Package with id $pkgId not found in the package metadata snapshot"
        ),
      )
      if (
        // If a package is vetted or it is not a schema package, we continue with checking its dependencies.
        // We ignore unvetted non-schema packages to support
        // disjoint versions across informees (e.g. Daml stdlib packages)
        allVettedPackages(pkgId) || !isSchemaPackage(pkg)
      ) {
        val dependencies = dependencyGraph(pkgId)

        dependencies.foldLeft(Right(()): Either[LfPackageId, Unit]) {
          case (Right(()), dep) => allDepsVettedForCached.getOrElseUpdate(dep, isDeeplyVetted(dep))
          case (left, _) => left
        }
      } else {
        // If the schema package is not vetted, return it as an error
        Left(pkgId)
      }
    }

    candidatesForPackageName.view
      .mapValues(
        _.flatMap { candidates =>
          val (packagesWithUnvettedDeps, candidatesWithVettedDeps) = candidates.view
            .map(pkgRef =>
              isDeeplyVetted(pkgRef.pkgId).left.map(pkgRef.pkgId -> _).map(_ => pkgRef)
            )
            .toList
            .separate

          val lazyPackageRefsWithUnvettedDepsForError = packagesWithUnvettedDeps.view
            .map { case (pkg, unvettedDep) =>
              s"${pkg.toPackageReference(packageIndex).map(_.show).getOrElse(pkg.show)} -> ${unvettedDep.toPackageReference(packageIndex).map(_.show).getOrElse(pkg.show)}"
            }

          NonEmpty
            .from(candidatesWithVettedDeps.to(SortedSet))
            .toRight(
              show"Packages with dependencies not vetted by all interested parties: ${lazyPackageRefsWithUnvettedDepsForError.toList}"
            )
        }
      )
  }

  private def filterPackages(
      packageFilter: PackageFilter,
      candidatePackagesForName: MapView[LfPackageName, Candidate[SortedPreferences]],
  ): MapView[LfPackageName, Candidate[SortedPreferences]] =
    candidatePackagesForName.view
      .mapValues(_.flatMap { packageRefs =>
        NonEmpty
          .from(packageRefs.forgetNE.filter(ref => packageFilter(ref.packageName, ref.pkgId)))
          .toRight(
            show"All candidates discarded after applying package-id filter.\nCandidates: ${packageRefs
                .map(_.pkgId)}\nFilter: $packageFilter"
          )
      })

  private def resolveAndOrderPackageReferences(
      pkgIds: Set[LfPackageId],
      packageIndex: PackageIndex,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): Map[LfPackageName, Candidate[SortedPreferences]] =
    pkgIds.view
      .flatMap { pkgId =>
        pkgId
          .toPackageReference(packageIndex)
          .tap { pkgRefO =>
            if (pkgRefO.isEmpty)
              logger.trace(
                show"Discarding package ID $pkgId as it doesn't exist in the participant's package store."
              )
          }
      }
      .groupBy(_.packageName)
      .view
      .map { case (pkgName, pkgRefs) =>
        pkgName -> Right(
          NonEmpty
            .from(SortedSet.from(pkgRefs))
            // The groupBy in this chain ensures non-empty pkgRefs
            .getOrElse(
              sys.error(
                "Empty package references. This is likely a programming error. Please contact support"
              )
            )
        )
      }
      .toMap

  private def computePartyPackageCandidatesIntersection(
      candidatesPerParty: MapView[LfPartyId, Map[LfPackageName, Candidate[SortedPreferences]]]
  ): Map[LfPackageName, Candidate[SortedPreferences]] =
    candidatesPerParty.view
      .flatMap { case (party, pkgNameCandidates) => pkgNameCandidates.view.map(party -> _) }
      .foldLeft(Map.empty[LfPackageName, Candidate[SortedPreferences]]) {
        case (acc, (party, (pkgName, newCandidates))) =>
          acc.updatedWith(pkgName) {
            case None => Some(newCandidates)
            case Some(existingCandidates) =>
              Some(
                for {
                  existingPkgRefs <- existingCandidates
                  pkgRefs <- newCandidates
                  newPkgRefs <- NonEmpty
                    .from(existingPkgRefs.forgetNE.intersect(pkgRefs.forgetNE))
                    .toRight(
                      show"No package candidates for '$pkgName' after considering candidates for party $party.\nCurrent candidates: $existingPkgRefs.\nCandidates for party $party: $pkgRefs"
                    )
                } yield newPkgRefs
              )
          }
      }

  private def considerPartyPackageNameRequirements(
      candidatesPerPartyView: MapView[LfPartyId, Map[LfPackageName, Candidate[SortedPreferences]]],
      requirements: Map[LfPartyId, Set[LfPackageName]],
  ): MapView[LfPartyId, Map[LfPackageName, Candidate[SortedPreferences]]] = {
    val candidatesPerParty = candidatesPerPartyView.toMap

    requirements.view
      .foldLeft(candidatesPerParty) { case (acc, (party, requiredPackageNames)) =>
        acc.updatedWith(party) {
          // If all the required package-names are present in the candidates for the party,
          // all good
          case Some(availablePackageNames)
              if requiredPackageNames.subsetOf(availablePackageNames.keySet) =>
            Some(availablePackageNames)
          // If there are required package-names that are not present in the available candidates,
          // back-fill the candidates for the party with an error reason for each missing package-name
          case other: Option[Map[LfPackageName, Candidate[SortedPreferences]]] =>
            val availablePackageNames: Set[LfPackageName] = other.map(_.keySet).getOrElse(Set.empty)
            val unavailablePackageNames = requiredPackageNames.diff(availablePackageNames)
            Some(
              unavailablePackageNames.foldLeft(other.getOrElse(Map.empty)) {
                case (acc, missingRequiredPackageName) =>
                  acc.updated(
                    missingRequiredPackageName,
                    Left(
                      if (other.isEmpty)
                        show"Party $party is either not known on the synchronizer or it has no uniformly-vetted packages"
                      else
                        show"Party $party has no vetted packages for '$missingRequiredPackageName'"
                    ),
                  )
              }
            )
        }
      }
      .view
  }

  // Select the highest version package for each requested package-name
  // or discard the preference set if there is a requirement not satisfied
  private def selectRequestedPackages(
      candidates: MapView[LfPackageName, Candidate[SortedPreferences]],
      requiredPackageNames: Set[LfPackageName],
  ): Candidate[Set[PackageReference]] =
    requiredPackageNames.toList
      .foldM(Set.empty[PackageReference]) { case (acc, requestedPackageName) =>
        for {
          preferencesForNameE <- candidates
            .get(requestedPackageName)
            .toRight(
              show"No party has vetted a package and its dependencies on all its hosting participants for '$requestedPackageName'."
            )
          preferencesForName <- preferencesForNameE
        } yield acc + preferencesForName.last1
      }

  private def isSchemaPackage(pkg: Ast.PackageSignature): Boolean =
    pkg.modules.exists { case (_, module) =>
      module.interfaces.nonEmpty || module.templates.nonEmpty
    }
}
