// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.parallel.*
import com.digitalasset.canton
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError.UnableToQueryTopologySnapshot
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.routing.{
  AdmissibleSynchronizersComputation,
  RoutingSynchronizerStateFactory,
}
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError.NotConnectedToAnySynchronizer
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizersLookup,
  SyncServiceInjectionError,
}
import com.digitalasset.canton.topology.client.TopologySnapshotLoader
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.collection.View
import scala.concurrent.ExecutionContext

final class TopologyPackageMapBuilder(
    admissibleSynchronizersComputation: AdmissibleSynchronizersComputation,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  // Computes a SynchronizerId -> PartyId -> PackageId relation
  // that describes:
  //   - for each synchronizer-id that hosts all the provided `submitters` that can submit
  //   - which package-ids can be accepted (i.e. they are vetting-valid)
  //     in a transaction by each of the informees provided
  //   - if the prescribed synchronizer is provided, only that one is considered
  // TODO(#23334): Deduplicate with logic from SynchronizerSelector
  // TODO(#23334): Refactor to take as input the topology snapshots, so it goes in the direction of
  //                      using consistent snapshots during phase 1
  // TODO(#23334): Split the functionality in two phases: first restricts the synchronizers
  //                      based on the input submitters and informees; second computes the package maps
  def packageMapFor(
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      vettingValidityTimestamp: CantonTimestamp,
      prescribedSynchronizerIdO: Option[SynchronizerId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[SynchronizerId, Map[LfPartyId, Set[PackageId]]]] = {
    def assertSubmittersSubsetOfInformees: FutureUnlessShutdown[Unit] =
      if (submitters.subsetOf(informees)) FutureUnlessShutdown.unit
      else
        FutureUnlessShutdown.failed(
          new IllegalArgumentException(
            s"Submitters $submitters must be a subset of informees $informees"
          )
        )

    // TODO(#23334): Deduplicate logic with computeAdmissibleSynchronizers
    def validateAnySynchronizerReady: FutureUnlessShutdown[Unit] =
      if (
        connectedSynchronizersLookup.snapshot
          .exists { case (_synchronizerId, synchronizer) => synchronizer.ready }
      ) {
        FutureUnlessShutdown.unit
      } else
        FutureUnlessShutdown.failed(
          SyncServiceInjectionError.NotConnectedToAnySynchronizer.Error().asGrpcError
        )

    def getValidVettedPackages(
        synchronizerId: SynchronizerId,
        topoLoader: TopologySnapshotLoader,
        participants: Set[ParticipantId],
    ): FutureUnlessShutdown[(SynchronizerId, Map[ParticipantId, Set[PackageId]])] =
      participants.toList
        // TODO(#23334): Limit the load on the DB by either limiting the parallelism
        //                   or by introducing a batch load method on the TopologySnapshotLoader
        .parTraverse { participantId =>
          topoLoader
            .loadVettedPackages(participantId)
            .map { vettedPackages =>
              val packagesWithValidVetting = vettedPackages.view.collect {
                case (pkgId, vettedPackage) if vettedPackage.validAt(vettingValidityTimestamp) =>
                  pkgId
              }.toSet

              participantId -> packagesWithValidVetting
            }
        }
        .map(participantVettedPackages => synchronizerId -> participantVettedPackages.toMap)

    // TODO(#23334): Use a single synchronizer state snapshot throughout a submission
    val synchronizerState = RoutingSynchronizerStateFactory.create(connectedSynchronizersLookup)
    def computeAdmissibleSynchronizers
        : FutureUnlessShutdown[Map[SynchronizerId, TopologySnapshotLoader]] =
      prescribedSynchronizerIdO
        .map(syncId =>
          // Only consider the target synchronizer, if provided
          connectedSynchronizersLookup
            .get(syncId)
            .map(connectedSynchronizer =>
              Map(
                connectedSynchronizer.synchronizerId -> connectedSynchronizer.topologyClient.currentSnapshotApproximation
              )
            )
            .map(FutureUnlessShutdown.pure)
            .getOrElse(
              FutureUnlessShutdown
                .failed(UnableToQueryTopologySnapshot.Failed(syncId).asGrpcError)
            )
        )
        .getOrElse(
          admissibleSynchronizersComputation
            .forParties(submitters, informees, synchronizerState)
            .leftSemiflatMap(err => FutureUnlessShutdown.failed(err.asGrpcError))
            .merge
            .flatMap(synchronizers =>
              synchronizers.forgetNE.toSeq
                .parTraverse(syncId =>
                  connectedSynchronizersLookup
                    .get(syncId)
                    .toRight(
                      // TODO(#23334): This error is misleading but will be solved with using
                      //               a single synchronizer state snapshot
                      FutureUnlessShutdown.failed(NotConnectedToAnySynchronizer.Error().asGrpcError)
                    )
                    .map(FutureUnlessShutdown.pure)
                    .merge
                    .map(sync => syncId -> sync.topologyClient.currentSnapshotApproximation)
                )
                .map(_.toMap)
            )
        )

    for {
      _ <- assertSubmittersSubsetOfInformees
      _ <- validateAnySynchronizerReady
      admissibleSynchronizersSnapshot <- computeAdmissibleSynchronizers
      // Find on which of a synchronizer's participants are the informees hosted
      filteredTopologyView: Seq[
        (SynchronizerId, (TopologySnapshotLoader, Map[LfPartyId, Set[ParticipantId]]))
      ] <-
        admissibleSynchronizersSnapshot.iterator.toList.parTraverse { case (syncId, topoLoader) =>
          topoLoader
            .activeParticipantsOfParties(informees.toList)
            .map(participantsOfParties => syncId -> (topoLoader -> participantsOfParties))
        }

      // Compute the vetting state per participant with the validity computed at the provided `vettingValidityTimestamp`
      //   Note: a package is vetting-valid if there exists a VettedPackage reference in the VettedPackages topology transaction
      //         for which valid_from < vettingValidityTimestamp <= valid_until OR if the bounds are not defined
      synchronizersParticipantsVettingState: Map[
        SynchronizerId,
        Map[ParticipantId, Set[PackageId]],
      ] <-
        filteredTopologyView.view.toList
          .parTraverse { case (sync, (topoLoader, partyParticipants)) =>
            getValidVettedPackages(
              synchronizerId = sync,
              topoLoader = topoLoader,
              participants =
                partyParticipants.view.flatMap { case (_party, participants) => participants }.toSet,
            )
          }
          .map(_.toMap)
      globalPackageMap = computeGlobalPackageMap(
        partyAllocation =
          filteredTopologyView.view.map { case (syncId, (_topoLoader, participantsOfParties)) =>
            syncId -> participantsOfParties
          },
        participantVettingState = synchronizersParticipantsVettingState,
      )
    } yield globalPackageMap
  }

  private def computeGlobalPackageMap(
      partyAllocation: View[(SynchronizerId, Map[LfPartyId, Set[ParticipantId]])],
      participantVettingState: Map[SynchronizerId, Map[ParticipantId, Set[PackageId]]],
  ): Map[SynchronizerId, Map[LfPartyId, Set[PackageId]]] =
    partyAllocation.map { case (synchronizerId, partiesParticipants) =>
      synchronizerId -> partiesParticipants.view.map { case (party, hostingParticipants) =>
        val vettingStateIntersection = hostingParticipants.view
          .map(participantId =>
            canton.checked(participantVettingState(synchronizerId)(participantId))
          )
          // We only care about packages vetted on all hosting participants
          // Otherwise, a transaction using them will be rejected
          .reduceOption(_.intersect(_))
          // TODO(#23334): Empty set means that the party does not have any commonly vetting package-id
          //          across all its hosting participants. This probably spells trouble
          //          and the synchronizer-id must be disconsidered (maybe stale)
          .getOrElse(Set.empty)
        party -> vettingStateIntersection
      }.toMap
    }.toMap
}
