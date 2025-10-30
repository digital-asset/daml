// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.implicits.catsSyntaxSemigroup
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.participant.admin.AdminWorkflowServices
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags, PhysicalSynchronizerId}
import com.digitalasset.canton.util.SetupPackageVetting.AllUnvettingFlags
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.{BaseTest, LfPackageId}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref.PackageId
import org.scalatest.Assertions.fail
import org.scalatest.LoneElement.convertToCollectionLoneElementWrapper

import java.io.File

/** Declarative API for configuring the vetting state for a set of participants and their connected
  * synchronizers.
  *
  * @param darPaths
  *   The file paths to the DARs that need to be uploaded to the participants. A participant's need
  *   for a DAR is determined based on the target topology.
  * @param targetTopology
  *   The desired vetting topology for each synchronizer and its connected participants. The vetted
  *   packages for each participant must reference only the main package-ids of DARs listed in
  *   [[darPaths]]. All dependencies of a DAR are vetted without limits, unless a dependency is
  *   itself a main package-id of one of the DARs, in which case the bounds defined in its
  *   [[VettedPackage]] declaration are respected.
  */
class SetupPackageVetting(
    darPaths: Set[String /* DAR path */ ],
    targetTopology: Map[PhysicalSynchronizerId, Map[ParticipantReference, Set[VettedPackage]]],
) {
  def run(): Unit = {
    val participants = targetTopology.view.values.flatMap(_.keys).toSet
    val participantsPerSynchronizer: Map[PhysicalSynchronizerId, Set[ParticipantReference]] =
      targetTopology.view.map { case (sync, participantPackages) =>
        sync -> participantPackages.keySet
      }.toMap
    val synchronizersPerParticipant: Map[ParticipantReference, Set[PhysicalSynchronizerId]] =
      MapsUtil.transpose(participantsPerSynchronizer)

    val darsMap: Map[PackageId, (String, Set[PackageId])] = darPaths.view.map { darPath =>
      val dar = DarReader
        .readArchiveFromFile(darFile = new File(darPath))
        .fold(
          err => throw new IllegalArgumentException(s"Failed to read DAR: $darPath", err),
          identity,
        )

      dar.main.pkgId -> (darPath, dar.dependencies.map(_.pkgId).toSet)
    }.toMap

    // Clean-up
    unvetAllUserPackages(participants, synchronizersPerParticipant)

    // Upload the required DARs on each participant (without vetting)
    uploadDarsToParticipants(darsMap.view.mapValues(_._1).toMap)

    // Setup the target vetting state
    setupTargetVettingState(participantsPerSynchronizer, darsMap.view.mapValues(_._2).toMap)
  }

  private def setupTargetVettingState(
      participantsPerSynchronizer: Map[PhysicalSynchronizerId, Set[ParticipantReference]],
      darsMap: Map[PackageId, Set[PackageId]],
  ): Unit =
    targetTopology.foreach { case (synchronizerId, vettedPackagesPerParticipant) =>
      vettedPackagesPerParticipant
        // Do not issue vetting transactions with empty vetted packages set
        .filter(_._2.nonEmpty)
        .foreach { case (participant, vettedPackages) =>
          val packageIdsWithExplicitVetting = vettedPackages.map(_.packageId)
          val vettedPackagesAdditions = vettedPackages ++ {
            // Add the dependencies of all DAR main package-ids
            VettedPackage.unbounded(
              vettedPackages.view
                .map(_.packageId)
                .flatMap(darsMap)
                .toSeq
                .distinct
                // Do not set unbound vetting if the package-id was already explicitly vetted in the targetTopology
                // (e.g. a main DAR package-id can be a dependency of another DAR)
                .filterNot(packageIdsWithExplicitVetting)
            )
          }
          participant.topology.vetted_packages.propose_delta(
            participant = participant.id,
            store = synchronizerId,
            adds = vettedPackagesAdditions.toSeq,
          )

          val allParticipants = participantsPerSynchronizer(synchronizerId)

          allParticipants.foreach { participantToObserveTopology =>
            BaseTest.eventually() {
              val currentVettingState = participantToObserveTopology.topology.vetted_packages
                .list(
                  Some(synchronizerId),
                  filterParticipant = participant.id.filterString,
                )
                .loneElement
                .item
                .packages
                .toSet

              if (vettedPackagesAdditions.subsetOf(currentVettingState)) () else fail()
            }
          }
        }
    }

  private def uploadDarsToParticipants(darsMap: Map[PackageId, String]): Unit =
    targetTopology.view.values
      .reduceOption(_ |+| _)
      .getOrElse(Map.empty[ParticipantReference, Set[VettedPackage]])
      .foreach { case (participant, vettedPackages) =>
        vettedPackages
          .map(_.packageId)
          .map(darMainPkgId =>
            darsMap.getOrElse(
              darMainPkgId,
              throw new IllegalArgumentException(s"DAR for $darMainPkgId not found"),
            )
          )
          .foreach { darPath =>
            participant.dars.upload(
              darPath,
              synchronizerId = None,
              vetAllPackages = false,
              synchronizeVetting = false,
            )
          }
      }

  private def unvetAllUserPackages(
      participants: Set[ParticipantReference],
      synchronizersPerParticipant: Map[ParticipantReference, Set[PhysicalSynchronizerId]],
  ): Unit =
    participants.foreach { participant =>
      val defaultPackageIds = (Set(
        "daml-prim",
        "daml-stdlib",
        "daml-script",
        "ghc-stdlib",
      ) ++ AdminWorkflowServices.AdminWorkflowNames).flatMap(filterName =>
        withFailOnLimitHit(PositiveInt.tryCreate(1000), s"fetch SDK packages ($filterName)") {
          fetchLimit =>
            participant.packages.list(limit = fetchLimit, filterName = filterName).map(_.packageId)
        }
      )

      val userDefinedPackages =
        (withFailOnLimitHit(PositiveInt.tryCreate(1000), "fetch all packages") { fetchLimit =>
          participant.packages.list(limit = fetchLimit).map(_.packageId)
        }.toSet
          -- defaultPackageIds).map(LfPackageId.assertFromString)

      if (userDefinedPackages.nonEmpty) {
        // Remove the vetted packages from the authorized store
        participant.topology.vetted_packages.propose_delta(
          participant = participant.id,
          store = TopologyStoreId.Authorized,
          removes = userDefinedPackages.toSeq,
          force = AllUnvettingFlags,
        )

        // Remove the vetted packages from each synchronizer store
        synchronizersPerParticipant(participant).foreach { syncId =>
          participant.topology.vetted_packages.propose_delta(
            participant = participant.id,
            store = syncId,
            removes = userDefinedPackages.toSeq,
            force = AllUnvettingFlags,
          )
        }
      }
    }

  private def withFailOnLimitHit[T](fetchLimit: PositiveInt, opName: String)(
      f: PositiveInt => Seq[T]
  ): Seq[T] = {
    val result = f(fetchLimit)
    if (result.sizeIs == fetchLimit.value) fail(s"Request limit hit for $opName")
    else result
  }
}

object SetupPackageVetting {
  val AllUnvettingFlags: ForceFlags = ForceFlags(
    ForceFlag.AllowUnvettedDependencies
  )

  def apply(
      darPaths: Set[String],
      targetTopology: Map[PhysicalSynchronizerId, Map[ParticipantReference, Set[VettedPackage]]],
  ): Unit =
    new SetupPackageVetting(darPaths, targetTopology).run()
}
