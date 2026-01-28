// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.console.{
  ConsoleEnvironmentTestHelpers,
  InstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
  ParticipantReference,
  RemoteMediatorReference,
  RemoteParticipantReference,
  RemoteSequencerReference,
  SequencerReference,
}
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.PhysicalSynchronizerId

/** Aliases used by our typical single synchronizer and multi synchronizer tests. If a test attempts
  * to use an aliases against an environment without that node configured it will immediately throw.
  */
trait CommonTestAliases {
  this: EnvironmentTestHelpers with ConsoleEnvironmentTestHelpers =>

  def getInitializedSynchronizer(alias: SynchronizerAlias): InitializedSynchronizer =
    initializedSynchronizers
      .getOrElse(alias, throw new RuntimeException(s"Synchronizer `$alias` is not bootstrapped"))

  lazy val participant1: LocalParticipantReference = lp("participant1")
  lazy val participant1_ : ParticipantReference = p("participant1")
  lazy val participant2: LocalParticipantReference = lp("participant2")
  lazy val participant3: LocalParticipantReference = lp("participant3")
  lazy val participant4: LocalParticipantReference = lp("participant4")
  lazy val participant5: LocalParticipantReference = lp("participant5")
  lazy val participant6: LocalParticipantReference = lp("participant6")

  lazy val sequencer1: LocalSequencerReference = ls("sequencer1")
  lazy val sequencer1_ : SequencerReference = s("sequencer1")
  lazy val sequencer2: LocalSequencerReference = ls("sequencer2")
  lazy val sequencer3: LocalSequencerReference = ls("sequencer3")
  lazy val sequencer4: LocalSequencerReference = ls("sequencer4")
  lazy val sequencer5: LocalSequencerReference = ls("sequencer5")

  lazy val mediator1: LocalMediatorReference = lm("mediator1")
  lazy val mediator2: LocalMediatorReference = lm("mediator2")
  lazy val mediator3: LocalMediatorReference = lm("mediator3")
  lazy val mediator4: LocalMediatorReference = lm("mediator4")
  lazy val mediator5: LocalMediatorReference = lm("mediator5")

  // Remote
  lazy val remoteSequencer1: RemoteSequencerReference = rs("sequencer1")
  lazy val remoteSequencer2: RemoteSequencerReference = rs("sequencer2")
  lazy val remoteSequencer3: RemoteSequencerReference = rs("sequencer3")
  lazy val remoteSequencer4: RemoteSequencerReference = rs("sequencer4")

  lazy val remoteMediator1: RemoteMediatorReference = rm("mediator1")
  lazy val remoteMediator2: RemoteMediatorReference = rm("mediator2")

  lazy val remoteParticipant1: RemoteParticipantReference = rp("participant1")
  lazy val remoteParticipant2: RemoteParticipantReference = rp("participant2")

  // synchronizer1
  lazy val daName: SynchronizerAlias = SynchronizerAlias.tryCreate("synchronizer1")
  lazy val daId: PhysicalSynchronizerId = getInitializedSynchronizer(daName).physicalSynchronizerId
  lazy val synchronizer1Id: PhysicalSynchronizerId = daId
  lazy val staticSynchronizerParameters1: StaticSynchronizerParameters = getInitializedSynchronizer(
    daName
  ).staticSynchronizerParameters
  lazy val synchronizerOwners1: Set[InstanceReference] = getInitializedSynchronizer(
    daName
  ).synchronizerOwners

  // synchronizer2
  lazy val acmeName: SynchronizerAlias = SynchronizerAlias.tryCreate("synchronizer2")
  lazy val acmeId: PhysicalSynchronizerId = getInitializedSynchronizer(
    acmeName
  ).physicalSynchronizerId
  lazy val synchronizer2Id: PhysicalSynchronizerId = acmeId
  lazy val staticSynchronizerParameters2: StaticSynchronizerParameters = getInitializedSynchronizer(
    acmeName
  ).staticSynchronizerParameters
  lazy val synchronizerOwners2: Set[InstanceReference] = getInitializedSynchronizer(
    acmeName
  ).synchronizerOwners

  // synchronizer3
  lazy val repairSynchronizerName: SynchronizerAlias = SynchronizerAlias.tryCreate("synchronizer3")
  lazy val repairSynchronizerId: PhysicalSynchronizerId = getInitializedSynchronizer(
    repairSynchronizerName
  ).physicalSynchronizerId
  lazy val synchronizer3Id: PhysicalSynchronizerId = repairSynchronizerId
  lazy val synchronizerOwners3: Set[InstanceReference] = getInitializedSynchronizer(
    repairSynchronizerName
  ).synchronizerOwners

  // synchronizer4
  lazy val devSynchronizerName: SynchronizerAlias = SynchronizerAlias.tryCreate("synchronizer4")
  lazy val devSynchronizerId: PhysicalSynchronizerId = getInitializedSynchronizer(
    devSynchronizerName
  ).physicalSynchronizerId
}
