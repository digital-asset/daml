// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import cats.Order.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.PackageReference
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound.PackageNamesNotFound
import com.digitalasset.canton.ledger.participant.state.SyncService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPackageVersion, LfPartyId}
import com.digitalasset.daml.lf.data.Ref.PackageName

import scala.concurrent.ExecutionContext

class PackagePreferenceBackend(
    clock: Clock,
    adminParty: LfPartyId,
    syncService: SyncService,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Computes the commonly-preferred package version for the given parties and package name based
    * on the package vetting state.
    *
    * @param parties
    *   The parties whose package vetting state is considered
    * @param packageName
    *   The package name for which the preferred package version is requested
    * @param supportedPackageIds
    *   If provided, acts as a filter on the candidate package-ids for preference computation.
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
  def getPreferredPackageVersion(
      parties: Set[LfPartyId],
      packageName: PackageName,
      supportedPackageIds: Option[Set[LfPackageId]],
      synchronizerId: Option[SynchronizerId],
      vettingValidAt: Option[CantonTimestamp],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Option[(PackageReference, SynchronizerId)]] = {
    val routingSynchronizerState = syncService.getRoutingSynchronizerState
    val packageMetadataSnapshot = syncService.getPackageMetadataSnapshot
    val packageIdMapSnapshot = packageMetadataSnapshot.packageIdVersionMap

    def collectReferencesForTargetPackageName(
        pkgId: LfPackageId,
        packageIdMapSnapshot: Map[LfPackageId, (LfPackageName, LfPackageVersion)],
    ): Set[PackageReference] =
      packageIdMapSnapshot
        // Optionality is supported since utility packages are not in the packageIdMapSnapshot,
        // since they are not upgradable
        .get(pkgId)
        .iterator
        .collect {
          case (name, version) if name == packageName => PackageReference(pkgId, version, name)
        }
        .toSet

    def computePackagePreference(
        packageMap: Map[SynchronizerId, Map[LfPartyId, Set[LfPackageId]]]
    ): Option[(PackageReference, SynchronizerId)] =
      packageMap.view
        .flatMap { case (syncId, partyPackageMap: Map[LfPartyId, Set[LfPackageId]]) =>
          val uniformlyVettedPackages = partyPackageMap.values
            // Find all commonly vetted package-ids for the given parties for the current synchronizer (`syncId`)
            .reduceOption(_.intersect(_))
            .getOrElse(Set.empty[LfPackageId])

          supportedPackageIds
            .map(_.intersect(uniformlyVettedPackages))
            .getOrElse(uniformlyVettedPackages)
            .flatMap(collectReferencesForTargetPackageName(_, packageIdMapSnapshot))
            .map(_ -> syncId)
        }
        // There is (at most) a preferred package for each synchronizer
        // Pick the one with the highest version, if any
        // If two preferences match, pick according to synchronizer-id order
        // TODO(#23334): Consider using the synchronizer priority order to break ties
        //               However since the synchronizer priority is a participant-local concept,
        //               this might result in different outcomes depending on the participant used
        //               for rendering this list
        .maxOption(
          Ordering.Tuple2(
            implicitly[Ordering[PackageReference]],
            // Follow the pattern used for SynchronizerRank ordering,
            // where lexicographic order picks the most preferred synchronizer by id
            implicitly[Ordering[SynchronizerId]].reverse,
          )
        )

    for {
      _ <-
        if (packageMetadataSnapshot.packageNameMap.contains(packageName)) FutureUnlessShutdown.unit
        else
          FutureUnlessShutdown.failed(PackageNamesNotFound.Reject(Set(packageName)).asGrpcError)
      packageMapForRequest <- syncService
        .packageMapFor(
          submitters = Set.empty,
          informees = parties,
          vettingValidityTimestamp = vettingValidAt.getOrElse(clock.now),
          prescribedSynchronizer = synchronizerId,
          routingSynchronizerState = routingSynchronizerState,
        )

      packagePreference = computePackagePreference(packageMapForRequest)
    } yield packagePreference
  }

  def getPreferredPackageVersionForParticipant(
      packageName: PackageName,
      supportedPackageIds: Set[LfPackageId],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Option[LfPackageId]] =
    getPreferredPackageVersion(
      parties = Set(adminParty),
      packageName = packageName,
      supportedPackageIds = Some(supportedPackageIds),
      synchronizerId = None,
      vettingValidAt = Some(CantonTimestamp.MaxValue),
    )
      .map(_.map(_._1.pkdId))
}
