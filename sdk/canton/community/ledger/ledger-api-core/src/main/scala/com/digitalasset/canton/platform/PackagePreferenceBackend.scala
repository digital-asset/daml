// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import cats.implicits.{catsSyntaxAlternativeSeparate, toFoldableOps, toTraverseOps}
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
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.PackagePreferenceBackend.*
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPackageVersion, LfPartyId}

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
    * @param packageIdFilter
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
      packageIdFilter: PackageIdFilter,
      synchronizerId: Option[SynchronizerId],
      vettingValidAt: Option[CantonTimestamp],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Either[String, (Seq[PackageReference], PhysicalSynchronizerId)]] = {
    val routingSynchronizerState = syncService.getRoutingSynchronizerState
    val packageMetadataSnapshot = syncService.getPackageMetadataSnapshot
    val packageIndex = packageMetadataSnapshot.packageIdVersionMap

    for {
      _ <- ensurePackageNamesKnown(packageVettingRequirements, packageMetadataSnapshot)
      packageMapForRequest <- syncService.packageMapFor(
        submitters = Set.empty,
        informees = packageVettingRequirements.allParties,
        vettingValidityTimestamp = vettingValidAt.getOrElse(clock.now),
        prescribedSynchronizer = synchronizerId,
        routingSynchronizerState = routingSynchronizerState,
      )
      synchronizerCandidates = computePerSynchronizerPackagePreferenceSet(
        synchronizersPartiesVettingState = packageMapForRequest,
        packageIndex = packageIndex,
        requiredPackageNames = packageVettingRequirements.allPackageNames,
        requirements = MapsUtil.transpose(packageVettingRequirements.value),
        packageIdFilter = packageIdFilter,
      )
    } yield findValidCandidate(synchronizerCandidates)
  }

  private def findValidCandidate(
      synchronizerCandidates: Map[PhysicalSynchronizerId, Candidate[Set[PackageReference]]]
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
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
      packageIdFilter =
        PackageIdFilterRestriction(supportedPackageIds, supportedPackageIdsDescription),
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
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Unit] = {
    val requestPackageNames = packageVettingRequirements.allPackageNames
    val knownPackageNames = packageMetadataSnapshot.packageNameMap.keySet
    val unknownPackageNames = requestPackageNames.diff(knownPackageNames)

    if (unknownPackageNames.isEmpty) FutureUnlessShutdown.unit
    else
      FutureUnlessShutdown.failed(
        PackageNamesNotFound.Reject(unknownPackageNames).asGrpcError
      )
  }

  private def computePerSynchronizerPackagePreferenceSet(
      synchronizersPartiesVettingState: Map[PhysicalSynchronizerId, Map[LfPartyId, Set[
        LfPackageId
      ]]],
      packageIndex: PackageIndex,
      requiredPackageNames: Set[LfPackageName],
      requirements: Map[LfPartyId, Set[LfPackageName]],
      packageIdFilter: PackageIdFilter,
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Map[PhysicalSynchronizerId, Candidate[Set[PackageReference]]] =
    synchronizersPartiesVettingState.view
      .mapValues(
        _.view
          // Resolve to full package references
          .mapValues(resolveAndOrderPackageReferences(_, packageIndex))
          .pipe((candidates: MapView[LfPartyId, Map[LfPackageName, NonEmpty[SortedPreferences]]]) =>
            // Filter out synchronizers for which there is no vetted package satisfying a vetting requirement (party <-> package-name)
            preserveSatisfyingPartyPackageNameRequirements(candidates, requirements)
          )
          .map {
            (candidates: MapView[LfPartyId, Map[LfPackageName, NonEmpty[SortedPreferences]]]) =>
              // At this point we are reducing the party dimension by
              // intersecting all package-ids for a package-name of a party with the same for other parties.
              preserveSatisfyingPartyPackageCandidatesIntersection(candidates)
          }
          // Preserve only the candidate package-ids that are vetted and all their dependencies are vetted
          .map(preserveDeeplyVetted(packageIndex))
          // Apply package-id filter restriction to the candidate package-ids
          // Note: the filter can discard packages that are dependencies of other candidates
          .map(filterPackages(packageIdFilter))
          .flatMap((candidates: MapView[LfPackageName, Candidate[NonEmpty[SortedPreferences]]]) =>
            // Select the highest version package for each requested package-name
            selectRequestedPackages(candidates, requiredPackageNames)
          )
      )
      .toMap

  // Preserve for each package-name all the package-ids that are vetted and all their dependencies are vetted
  private def preserveDeeplyVetted(packageIndex: PackageIndex)(
      candidatesForPackageName: Map[LfPackageName, Candidate[NonEmpty[SortedPreferences]]]
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): MapView[LfPackageName, Candidate[NonEmpty[SortedPreferences]]] = {
    val packageMetadataSnapshot = syncService.getPackageMetadataSnapshot
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
    def allDepsVettedFor(pkgId: LfPackageId): Either[LfPackageId, Unit] = {
      val dependencies = dependencyGraph(pkgId)

      dependencies.find(!allVettedPackages(_)) match {
        case Some(notVetted) => Left(notVetted)
        case None =>
          dependencies.foldLeft(Right(()): Either[LfPackageId, Unit]) {
            case (Right(()), dep) =>
              allDepsVettedForCached.getOrElseUpdate(dep, allDepsVettedFor(dep))
            case (left, _) => left
          }
      }
    }

    candidatesForPackageName.view
      .mapValues(
        _.flatMap { candidates =>
          val (packagesWithUnvettedDeps, candidatesWithVettedDeps) = candidates.view
            .map(pkgRef =>
              allDepsVettedFor(pkgRef.pkgId).left.map(pkgRef.pkgId -> _).map(_ => pkgRef)
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

  private def filterPackages(packageIdFilter: PackageIdFilter)(
      candidatePackagesForName: MapView[LfPackageName, Candidate[
        NonEmpty[SortedPreferences /* most preferred last */ ]
      ]]
  ): MapView[LfPackageName, Candidate[NonEmpty[SortedPreferences /* most preferred last */ ]]] =
    candidatePackagesForName.view
      .mapValues(_.flatMap { packageRefs =>
        NonEmpty
          .from(packageRefs.forgetNE.filter(ref => packageIdFilter(ref.pkgId)))
          .toRight(
            show"All candidates discarded after applying package-id filter.\nCandidates: ${packageRefs
                .map(_.pkgId)}\nFilter: $packageIdFilter"
          )
      })

  private def resolveAndOrderPackageReferences(
      pkgIds: Set[LfPackageId],
      packageIndex: PackageIndex,
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Map[LfPackageName, NonEmpty[SortedPreferences /* most preferred last */ ]] =
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
        pkgName -> NonEmpty
          .from(SortedSet.from(pkgRefs))
          // The groupBy in this chain ensures non-empty pkgRefs
          .getOrElse(
            sys.error(
              "Empty package references. This is likely a programming error. Please contact support"
            )
          )
      }
      .toMap

  private def preserveSatisfyingPartyPackageCandidatesIntersection(
      candidatesPerParty: MapView[LfPartyId, Map[LfPackageName, NonEmpty[SortedPreferences]]]
  ): Map[LfPackageName, Candidate[NonEmpty[SortedPreferences]]] =
    candidatesPerParty.view
      .flatMap { case (party, pkgNameCandidates) => pkgNameCandidates.view.map(party -> _) }
      .foldLeft(Map.empty[LfPackageName, Candidate[NonEmpty[SortedPreferences]]]) {
        case (acc, (party, (pkgName, pkgReferences))) =>
          acc.updatedWith(pkgName) {
            case None => Some(Right(pkgReferences))
            case Some(existing) =>
              Some(existing.flatMap { existingPkgRefs =>
                NonEmpty
                  .from(existingPkgRefs.forgetNE.intersect(pkgReferences.forgetNE))
                  .toRight(
                    show"No package candidates for '$pkgName' after intersection with candidates from party $party.\nCurrent candidates: $existingPkgRefs.\nCandidates for party $party: $pkgReferences"
                  )
              })
          }
      }

  private def selectRequestedPackages(
      candidates: MapView[LfPackageName, Candidate[
        NonEmpty[SortedPreferences /* most preferred last */ ]
      ]],
      requiredPackageNames: Set[LfPackageName],
  ): Candidate[Set[PackageReference]] =
    requiredPackageNames.toList
      .foldM(Set.empty[PackageReference]) { case (acc, requestedPackageName) =>
        for {
          preferencesForNameE <- candidates
            .get(requestedPackageName)
            .toRight(show"No package candidates for '$requestedPackageName''")
          preferencesForName <- preferencesForNameE
        } yield acc + preferencesForName.last1
      }

  private def preserveSatisfyingPartyPackageNameRequirements(
      candidatesPerParty: MapView[LfPartyId, Map[LfPackageName, NonEmpty[SortedPreferences]]],
      requirements: Map[LfPartyId, Set[LfPackageName]],
  ): Candidate[MapView[LfPartyId, Map[LfPackageName, NonEmpty[SortedPreferences]]]] =
    requirements.toSeq
      .traverse { case (party, requiredPackageNames) =>
        candidatesPerParty
          .get(party)
          .toRight(show"Party $party has no vetted package candidates")
          .flatMap { packagesForName =>
            val requiredWithoutCandidates = requiredPackageNames.diff(packagesForName.keySet)
            Either.cond(
              requiredWithoutCandidates.isEmpty,
              (),
              show"Party $party requires package-names with no vetted candidates: $requiredWithoutCandidates",
            )
          }
      }
      .map(_ => candidatesPerParty)
}

object PackagePreferenceBackend {
  private type SortedPreferences = SortedSet[PackageReference]
  private type PackageIndex = Map[LfPackageId, (LfPackageName, LfPackageVersion)]
  // A candidate refers to a value T that:
  //   - wraps the value in a Right if it is valid for package preferences computation OR
  //   - wraps the value's discarded reason in a Left
  private type Candidate[T] = Either[String, T]

  sealed trait PackageIdFilter extends Product with Serializable with PrettyPrinting {
    def apply(pkgId: LfPackageId): Boolean
  }

  case object AllowAllPackageIds extends PackageIdFilter {
    def apply(pkgId: LfPackageId): Boolean = true

    override protected def pretty: Pretty[AllowAllPackageIds.this.type] =
      Pretty.prettyOfString(_ => "All package-ids supported")
  }

  final case class PackageIdFilterRestriction(
      supportedPackageIds: Set[LfPackageId],
      restrictionDescription: String,
  ) extends PackageIdFilter {
    def apply(pkgId: LfPackageId): Boolean = supportedPackageIds(pkgId)
    override protected def pretty: Pretty[PackageIdFilterRestriction] =
      Pretty.prettyOfString(_ => show"$restrictionDescription: $supportedPackageIds")
  }
}
