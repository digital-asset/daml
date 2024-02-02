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
  RemoteSequencerNodeReference,
  SequencerNodeReference,
}

/** Aliases used by our typical single domain and multi domain tests.
  * If a test attempts to use an aliases against an environment without
  * that node configured it will immediately throw.
  */
trait CommonTestAliases[+CE <: ConsoleEnvironment] {
  this: ConsoleEnvironmentTestHelpers[CE] =>
  lazy val participant1x: LocalParticipantReference = lpx("participant1")
  lazy val participant1x_ : ParticipantReference = px("participant1")
  lazy val participant2x: LocalParticipantReference = lpx("participant2")
  lazy val participant3x: LocalParticipantReference = lpx("participant3")
  lazy val participant4x: LocalParticipantReference = lpx("participant4")
  lazy val participant5x: LocalParticipantReference = lpx("participant5")

  lazy val sequencer1x: LocalSequencerNodeReference = lsx("sequencer1")
  lazy val sequencer1x_ : SequencerNodeReference = sx("sequencer1")
  lazy val sequencer2x: LocalSequencerNodeReference = lsx("sequencer2")
  lazy val sequencer3x: LocalSequencerNodeReference = lsx("sequencer3")
  lazy val sequencer4x: LocalSequencerNodeReference = lsx("sequencer4")

  // Remote
  lazy val remoteSequencer1x: RemoteSequencerNodeReference = rsx("sequencer1")
  lazy val remoteSequencer2x: RemoteSequencerNodeReference = rsx("sequencer2")
  lazy val remoteSequencer3x: RemoteSequencerNodeReference = rsx("sequencer3")
  lazy val remoteMediator1x: RemoteMediatorReference = rmx("mediator1")

  lazy val mediator1x: LocalMediatorReference = lmx("mediator1")
  lazy val mediator2x: LocalMediatorReference = lmx("mediator2")
  lazy val mediator3x: LocalMediatorReference = lmx("mediator3")
  lazy val mediator4x: LocalMediatorReference = lmx("mediator4")

}
