// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.implicits.catsSyntaxParallelTraverse1
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.ledger.participant.state.RoutingSynchronizerState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.routing.AdmissibleSynchronizersComputation
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.ExecutionContext

final class PartyVettingMapComputation(
    admissibleSynchronizersComputation: AdmissibleSynchronizersComputation,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Computes per-synchronizer and party the set of vetted packages as follows:
    *   - For each admissible synchronizer (see
    *     [[com.digitalasset.canton.participant.protocol.submission.routing.AdmissibleSynchronizersComputation]])
    *     for the provided submitters and informees
    *   - For each informee taken from the union of submitters and informees, the set of packages
    *     that are vetting-valid on all its hosting participants
    *   - If the prescribed synchronizer is provided, only that one is considered
    *
    * @param submitters
    *   The parties that must have submission rights on the admissible synchronizers
    * @param informees
    *   The parties that must have at least observation rights on the admissible synchronizers
    * @param vettingValidityTimestamp
    *   The vetting validity timestamp to consider when checking the vetting status of packages
    * @param prescribedSynchronizerIdO
    *   An optional prescribed synchronizer id to restrict the synchronizer search space to
    * @param synchronizerState
    *   The current routing synchronizer state to be used for the computation
    *
    * TODO(#25385): [Tech-debt]: Split the functionality in two phases: first restricts the
    * synchronizers based on the input submitters and informees; second computes the package maps
    */
  def computePartyVettingMap(
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      vettingValidityTimestamp: CantonTimestamp,
      prescribedSynchronizerIdO: Option[SynchronizerId],
      synchronizerState: RoutingSynchronizerState,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[PhysicalSynchronizerId, Map[LfPartyId, Set[PackageId]]]] = {
    def validateAnySynchronizerReady: FutureUnlessShutdown[Unit] =
      if (synchronizerState.existsReadySynchronizer()) FutureUnlessShutdown.unit
      else
        FutureUnlessShutdown.failed(
          SyncServiceInjectionError.NotConnectedToAnySynchronizer.Error().asGrpcError
        )

    def toPhysicalSyncId(id: SynchronizerId): FutureUnlessShutdown[PhysicalSynchronizerId] =
      synchronizerState
        .getPhysicalId(id)
        .map(FutureUnlessShutdown.pure)
        .getOrElse(
          FutureUnlessShutdown.failed(
            InvalidPrescribedSynchronizerId
              .Generic(
                id,
                s"cannot resolve to physical synchronizer; ensure the node is connected to $id",
              )
              .asGrpcError
          )
        )

    def getPartyHostingOnAdmissibleSynchronizers(
        prescribedPSIdO: Option[PhysicalSynchronizerId]
    ): FutureUnlessShutdown[
      Map[PhysicalSynchronizerId, Map[LfPartyId, Set[ParticipantId]]]
    ] =
      for {
        // TODO(#28758): Use prescribed sync in admissibleSynchronizersComputation
        //               to reduce the search space in there
        partyHostingOnAllAdmissibleSyncs <- admissibleSynchronizersComputation
          .forParties(submitters, informees, synchronizerState)
          .leftSemiflatMap(err => FutureUnlessShutdown.failed(err.asGrpcError))
          .merge
        withPrescribedPsidConsidered <- prescribedPSIdO match {
          case Some(psid) =>
            val admissibleSyncshronizers = partyHostingOnAllAdmissibleSyncs.keySet
            partyHostingOnAllAdmissibleSyncs
              .get(psid)
              .map(partyHosting => FutureUnlessShutdown.pure(Map(psid -> partyHosting)))
              .getOrElse(
                FutureUnlessShutdown.failed(
                  InvalidPrescribedSynchronizerId
                    .NotAllInformeeAreOnSynchronizer(
                      physicalSynchronizerId = psid,
                      synchronizersOfAllInformees = admissibleSyncshronizers,
                    )
                    .asGrpcError
                )
              )
          case None => FutureUnlessShutdown.pure(partyHostingOnAllAdmissibleSyncs.forgetNE)
        }
      } yield withPrescribedPsidConsidered

    for {
      _ <- validateAnySynchronizerReady
      prescribedPSIdO <- prescribedSynchronizerIdO.traverse(toPhysicalSyncId)

      // Get the hosting participants for parties on the admissible synchronizers
      partyHostingOnAdmissibleSyncs <- getPartyHostingOnAdmissibleSynchronizers(prescribedPSIdO)

      // Compute the vetting state per participant with the validity computed at the provided `vettingValidityTimestamp`
      //   Note: a package is vetting-valid if there exists a VettedPackage reference in the VettedPackages topology transaction
      //         for which valid_from < vettingValidityTimestamp <= valid_until OR if the bounds are not defined
      perParticipantVettingState: Map[PhysicalSynchronizerId, Map[ParticipantId, Set[PackageId]]] <-
        partyHostingOnAdmissibleSyncs.toSeq
          .parTraverse { case (sync, partyParticipants) =>
            computePerParticipantVettingState(
              synchronizerState = synchronizerState,
              sync = sync,
              involvedParticipants = partyParticipants.view.values.flatten.toSet,
              vettingValidityTimestamp = vettingValidityTimestamp,
            )
          }
          .map(_.toMap)
      perPartyVettingState = computePerPartyVettingState(
        partyAllocation = partyHostingOnAdmissibleSyncs,
        perParticipantVettingState = perParticipantVettingState,
      )
    } yield perPartyVettingState
  }

  private def computePerParticipantVettingState(
      synchronizerState: RoutingSynchronizerState,
      sync: PhysicalSynchronizerId,
      involvedParticipants: Set[ParticipantId],
      vettingValidityTimestamp: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(PhysicalSynchronizerId, Map[ParticipantId, Set[PackageId]])] =
    for {
      topologySnapshot <- FutureUnlessShutdown
        .fromTry(synchronizerState.getTopologySnapshotFor(sync).left.map(_.asGrpcError).toTry)
      vettedPackages <- topologySnapshot.loadVettedPackages(involvedParticipants)
      validVettedPackages = vettedPackages.view.mapValues {
        _.collect {
          case (pkgId, vettedPackage) if vettedPackage.validAt(vettingValidityTimestamp) => pkgId
        }.toSet
      }.toMap
    } yield sync -> validVettedPackages

  private def computePerPartyVettingState(
      partyAllocation: Map[PhysicalSynchronizerId, Map[LfPartyId, Set[ParticipantId]]],
      perParticipantVettingState: Map[PhysicalSynchronizerId, Map[ParticipantId, Set[PackageId]]],
  )(implicit tc: TraceContext): Map[PhysicalSynchronizerId, Map[LfPartyId, Set[PackageId]]] =
    partyAllocation.view.map { case (synchronizerId, partiesParticipants) =>
      synchronizerId -> partiesParticipants.view.map { case (party, hostingParticipants) =>
        val vettedPackageSets = hostingParticipants.view
          .map(participantId =>
            perParticipantVettingState(synchronizerId).getOrElse(participantId, Set.empty)
          )
        val packagesVettedOnAnyParticipant = vettedPackageSets.flatten.toSet

        val commonlyVettedPackages = vettedPackageSets
          // We only care about packages vetted on all hosting participants
          // Otherwise, a transaction using them will be rejected
          .reduceOption(_.intersect(_))
          .getOrElse(Set.empty)
        val discardedPackagesForParty = packagesVettedOnAnyParticipant -- commonlyVettedPackages
        if (discardedPackagesForParty.nonEmpty) {
          logger.debug(
            show"Discarding the following packages from package selection as they are not vetted on all hosting participants of party $party on synchronizer $synchronizerId: $discardedPackagesForParty"
          )
        }
        party -> commonlyVettedPackages
      }.toMap
    }.toMap
}
