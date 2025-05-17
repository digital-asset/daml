// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.error.TransactionRoutingError.UnableToQueryTopologySnapshot
import com.digitalasset.canton.ledger.participant.state.RoutingSynchronizerState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.routing.AdmissibleSynchronizersComputation
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError
import com.digitalasset.canton.topology.client.TopologySnapshotLoader
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.collection.View
import scala.concurrent.ExecutionContext

final class TopologyPackageMapBuilder(
    admissibleSynchronizersComputation: AdmissibleSynchronizersComputation,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  // Computes a SynchronizerId -> PartyId -> PackageId relation
  // that describes:
  //   - for each synchronizer-id that hosts all the provided `submitters` that can submit
  //   - which package-ids can be accepted (i.e. they are vetting-valid)
  //     in a transaction by each of the informees provided
  //   - if the prescribed synchronizer is provided, only that one is considered
  // TODO(#25385): Deduplicate with logic from SynchronizerSelector
  // TODO(#25385): Split the functionality in two phases: first restricts the synchronizers
  //                      based on the input submitters and informees; second computes the package maps
  def packageMapFor(
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      vettingValidityTimestamp: CantonTimestamp,
      prescribedSynchronizerIdO: Option[SynchronizerId],
      synchronizerState: RoutingSynchronizerState,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[PhysicalSynchronizerId, Map[LfPartyId, Set[PackageId]]]] = {
    def assertSubmittersSubsetOfInformees: FutureUnlessShutdown[Unit] =
      if (submitters.subsetOf(informees)) FutureUnlessShutdown.unit
      else
        FutureUnlessShutdown.failed(
          new IllegalArgumentException(
            s"Submitters $submitters must be a subset of informees $informees"
          )
        )

    def validateAnySynchronizerReady: FutureUnlessShutdown[Unit] =
      if (synchronizerState.existsReadySynchronizer()) FutureUnlessShutdown.unit
      else
        FutureUnlessShutdown.failed(
          SyncServiceInjectionError.NotConnectedToAnySynchronizer.Error().asGrpcError
        )

    def getValidVettedPackages(
        topoLoader: TopologySnapshotLoader,
        participants: Set[ParticipantId],
    ): FutureUnlessShutdown[Map[ParticipantId, Set[PackageId]]] =
      topoLoader
        .loadVettedPackages(participants.toSeq)
        .map(participantsPackagesVetted =>
          participantsPackagesVetted.view.mapValues { vettedPackages =>
            vettedPackages.view.collect {
              case (pkgId, vettedPackage) if vettedPackage.validAt(vettingValidityTimestamp) =>
                pkgId
            }.toSet
          }.toMap
        )

    def computeAdmissibleSynchronizers(
        prescribedPSIdO: Option[PhysicalSynchronizerId]
    ): FutureUnlessShutdown[Map[PhysicalSynchronizerId, TopologySnapshotLoader]] =
      prescribedPSIdO
        .map(psid =>
          // Only consider the target synchronizer, if provided
          synchronizerState.topologySnapshots
            .get(psid)
            .map(topologySnapshotLoader => Map(psid -> topologySnapshotLoader))
            .map(FutureUnlessShutdown.pure)
            .getOrElse(
              FutureUnlessShutdown
                .failed(UnableToQueryTopologySnapshot.Failed(psid).asGrpcError)
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
                  synchronizerState.topologySnapshots
                    .get(syncId)
                    .toRight(
                      // syncId was computed based on the synchronizer state, so it must be present in the enclosed topology snapshots
                      FutureUnlessShutdown.failed(
                        new IllegalStateException(s"Topology snapshot for $syncId not found")
                      )
                    )
                    .map(FutureUnlessShutdown.pure)
                    .merge
                    .map(topologySnapshotLoader => syncId -> topologySnapshotLoader)
                )
                .map(_.toMap)
            )
        )

    for {
      _ <- assertSubmittersSubsetOfInformees
      _ <- validateAnySynchronizerReady
      prescribedPSIdO <- prescribedSynchronizerIdO.traverse(id =>
        synchronizerState
          .getPhysicalId(id)
          .map(FutureUnlessShutdown.pure)
          .getOrElse(
            FutureUnlessShutdown.failed(
              InvalidPrescribedSynchronizerId
                .Generic(id, "cannot resolve to physical synchronizer")
                .asGrpcError
            )
          )
      )
      admissibleSynchronizersSnapshot <- computeAdmissibleSynchronizers(prescribedPSIdO)
      // Find on which of a synchronizer's participants are the informees hosted
      filteredTopologyView: Seq[
        (PhysicalSynchronizerId, (TopologySnapshotLoader, Map[LfPartyId, Set[ParticipantId]]))
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
        PhysicalSynchronizerId,
        Map[ParticipantId, Set[PackageId]],
      ] <-
        filteredTopologyView
          .parTraverse { case (sync, (topoLoader, partyParticipants)) =>
            getValidVettedPackages(
              topoLoader = topoLoader,
              participants = partyParticipants.view.flatMap { case (_party, participants) =>
                participants
              }.toSet,
            ).map(sync -> _)
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
      partyAllocation: View[(PhysicalSynchronizerId, Map[LfPartyId, Set[ParticipantId]])],
      participantVettingState: Map[PhysicalSynchronizerId, Map[ParticipantId, Set[PackageId]]],
  ): Map[PhysicalSynchronizerId, Map[LfPartyId, Set[PackageId]]] =
    partyAllocation.map { case (synchronizerId, partiesParticipants) =>
      synchronizerId -> partiesParticipants.view.map { case (party, hostingParticipants) =>
        val vettingStateIntersection = hostingParticipants.view
          .map(participantId =>
            canton.checked(participantVettingState(synchronizerId)(participantId))
          )
          // We only care about packages vetted on all hosting participants
          // Otherwise, a transaction using them will be rejected
          .reduceOption(_.intersect(_))
          // TODO(#25385): Empty set means that the party does not have any commonly vetting package-id
          //          across all its hosting participants. This probably spells trouble
          //          and the synchronizer-id must be disconsidered (maybe stale)
          .getOrElse(Set.empty)
        party -> vettingStateIntersection
      }.toMap
    }.toMap
}
