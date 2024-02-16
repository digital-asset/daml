// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleEnvironmentTestHelpers,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerNodeReference,
  ParticipantReference,
  RemoteMediatorReference,
  RemoteParticipantReference,
  RemoteSequencerNodeReference,
  SequencerNodeReference,
}

/** Aliases used by our typical single domain and multi domain tests.
  * If a test attempts to use an aliases against an environment without
  * that node configured it will immediately throw.
  */
trait CommonTestAliases[+CE <: ConsoleEnvironment] {
  this: ConsoleEnvironmentTestHelpers[CE] =>
  lazy val participant1: LocalParticipantReference = lpx("participant1")
  lazy val participant1_ : ParticipantReference = px("participant1")
  lazy val participant2: LocalParticipantReference = lpx("participant2")
  lazy val participant3: LocalParticipantReference = lpx("participant3")
  lazy val participant4: LocalParticipantReference = lpx("participant4")
  lazy val participant5: LocalParticipantReference = lpx("participant5")

  lazy val sequencer1: LocalSequencerNodeReference = lsx("sequencer1")
  lazy val sequencer1_ : SequencerNodeReference = sx("sequencer1")
  lazy val sequencer2: LocalSequencerNodeReference = lsx("sequencer2")
  lazy val sequencer3: LocalSequencerNodeReference = lsx("sequencer3")
  lazy val sequencer4: LocalSequencerNodeReference = lsx("sequencer4")

  // Remote
  lazy val remoteSequencer1: RemoteSequencerNodeReference = rsx("sequencer1")
  lazy val remoteSequencer2: RemoteSequencerNodeReference = rsx("sequencer2")
  lazy val remoteSequencer3: RemoteSequencerNodeReference = rsx("sequencer3")
  lazy val remoteMediator1: RemoteMediatorReference = rmx("mediator1")
  lazy val remoteParticipant1: RemoteParticipantReference = rpx("participant1")

  lazy val mediator1: LocalMediatorReference = lmx("mediator1")
  lazy val mediator2: LocalMediatorReference = lmx("mediator2")
  lazy val mediator3: LocalMediatorReference = lmx("mediator3")
  lazy val mediator4: LocalMediatorReference = lmx("mediator4")

}
