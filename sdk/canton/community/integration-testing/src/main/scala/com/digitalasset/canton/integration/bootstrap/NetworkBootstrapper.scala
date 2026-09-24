// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.bootstrap

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalInstanceReference,
  MediatorReference,
  SequencerReference,
}
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.sequencing.SubmissionRequestAmplification
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.{SynchronizerAlias, protocol}
import monocle.syntax.all.*

/** Bootstraps synchronizers given topology descriptions and stores information in
  * [[com.digitalasset.canton.integration.EnvironmentTestHelpers.initializedSynchronizers]].
  *
  * Starts all sequencers and mediators, and all participants that auto-initialize.
  */
class NetworkBootstrapper(networks: NetworkTopologyDescription*)(implicit
    env: TestConsoleEnvironment
) {
  def bootstrap(): Unit = {
    // Start all local nodes needed for bootstrap
    (networks.flatMap(_.synchronizerOwners) ++
      networks.flatMap(_.sequencers) ++
      networks.flatMap(_.mediators)).distinct.foreach {
      case n: LocalInstanceReference => n.start()
      case _ => // can't start a non local reference
    }

    networks.foreach(bootstrapSynchronizer)
  }

  private def bootstrapSynchronizer(desc: NetworkTopologyDescription): Unit = {
    val mediatorsToSequencers =
      desc.overrideMediatorToSequencers.getOrElse(
        desc.mediators.map(_ -> (desc.sequencers, PositiveInt.one, NonNegativeInt.zero)).toMap
      )

    val synchronizerId = env.bootstrap.synchronizer(
      synchronizerName = desc.synchronizerName,
      sequencers = desc.sequencers,
      mediatorsToSequencers = mediatorsToSequencers,
      synchronizerOwners = desc.synchronizerOwners,
      synchronizerThreshold = desc.synchronizerThreshold,
      staticSynchronizerParameters = desc.staticSynchronizerParameters,
      mediatorRequestAmplification = SubmissionRequestAmplification.NoAmplification,
      mediatorThreshold = desc.mediatorThreshold,
    )

    val synchronizerAlias = SynchronizerAlias.tryCreate(desc.synchronizerName)
    env.initializedSynchronizers.put(
      synchronizerAlias,
      InitializedSynchronizer(
        synchronizerId,
        desc.staticSynchronizerParameters.toInternal,
        synchronizerOwners = desc.synchronizerOwners.toSet,
      ),
    )
  }
}

object NetworkBootstrapper {
  def apply(networks: Seq[NetworkTopologyDescription])(implicit
      env: TestConsoleEnvironment
  ): NetworkBootstrapper = new NetworkBootstrapper(networks*)
}

/** @param overrideMediatorToSequencers
  *   By default, mediators connect to all sequencers. If set, the provided map will override the
  *   default behavior. The positive int defines the mediator's sequencer trust threshold.
  */
final case class NetworkTopologyDescription(
    synchronizerName: String,
    synchronizerOwners: Seq[InstanceReference],
    synchronizerThreshold: PositiveInt,
    sequencers: Seq[SequencerReference],
    mediators: Seq[MediatorReference],
    staticSynchronizerParameters: StaticSynchronizerParameters,
    mediatorRequestAmplification: SubmissionRequestAmplification,
    overrideMediatorToSequencers: Option[
      Map[MediatorReference, (Seq[SequencerReference], PositiveInt, NonNegativeInt)]
    ],
    mediatorThreshold: PositiveInt,
) {
  def withTopologyChangeDelay(
      topologyChangeDelay: NonNegativeFiniteDuration
  ): NetworkTopologyDescription =
    this
      .focus(_.staticSynchronizerParameters.topologyChangeDelay)
      .replace(topologyChangeDelay)
}

object NetworkTopologyDescription {

  def apply(
      synchronizerAlias: SynchronizerAlias,
      synchronizerOwners: Seq[InstanceReference],
      synchronizerThreshold: PositiveInt,
      sequencers: Seq[SequencerReference],
      mediators: Seq[MediatorReference],
      mediatorRequestAmplification: SubmissionRequestAmplification =
        SubmissionRequestAmplification.NoAmplification,
      overrideMediatorToSequencers: Option[
        Map[MediatorReference, (Seq[SequencerReference], PositiveInt, NonNegativeInt)]
      ] = None,
      mediatorThreshold: PositiveInt = PositiveInt.one,
  )(implicit env: TestConsoleEnvironment): NetworkTopologyDescription =
    NetworkTopologyDescription(
      synchronizerName = synchronizerAlias.unwrap,
      synchronizerOwners,
      synchronizerThreshold,
      sequencers,
      mediators,
      EnvironmentDefinition.defaultStaticSynchronizerParameters,
      mediatorRequestAmplification,
      overrideMediatorToSequencers,
      mediatorThreshold,
    )

  def createWithStaticSynchronizerParameters(
      synchronizerAlias: SynchronizerAlias,
      synchronizerOwners: Seq[InstanceReference],
      synchronizerThreshold: PositiveInt,
      sequencers: Seq[SequencerReference],
      mediators: Seq[MediatorReference],
      staticSynchronizerParameters: StaticSynchronizerParameters,
  ): NetworkTopologyDescription =
    NetworkTopologyDescription(
      synchronizerName = synchronizerAlias.unwrap,
      synchronizerOwners,
      synchronizerThreshold,
      sequencers,
      mediators,
      staticSynchronizerParameters,
      SubmissionRequestAmplification.NoAmplification,
      None,
      PositiveInt.one,
    )

}

/** A data container to hold useful information for initialized synchronizers
  */
final case class InitializedSynchronizer(
    physicalSynchronizerId: PhysicalSynchronizerId,
    staticSynchronizerParameters: protocol.StaticSynchronizerParameters,
    synchronizerOwners: Set[InstanceReference],
) {
  def synchronizerId: SynchronizerId = physicalSynchronizerId.logical
}
