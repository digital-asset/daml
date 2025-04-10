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

  def getPreferredPackageVersion(
      parties: Set[LfPartyId],
      packageName: PackageName,
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
          partyPackageMap.values
            // Find all commonly vetted package-ids for the given parties for the current synchronizer (`syncId`)
            .reduceOption(_.intersect(_))
            .getOrElse(Set.empty[LfPackageId])
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
      packageName: PackageName
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Option[LfPackageId]] =
    getPreferredPackageVersion(
      parties = Set(adminParty),
      packageName = packageName,
      synchronizerId = None,
      vettingValidAt = Some(CantonTimestamp.MaxValue),
    )
      .map(_.map(_._1.pkdId))
}
