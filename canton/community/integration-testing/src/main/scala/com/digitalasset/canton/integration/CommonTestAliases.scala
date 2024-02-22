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
  lazy val participant1: LocalParticipantReference = lp("participant1")
  lazy val participant1_ : ParticipantReference = p("participant1")
  lazy val participant2: LocalParticipantReference = lp("participant2")
  lazy val participant3: LocalParticipantReference = lp("participant3")
  lazy val participant4: LocalParticipantReference = lp("participant4")
  lazy val participant5: LocalParticipantReference = lp("participant5")

  lazy val sequencer1: LocalSequencerNodeReference = ls("sequencer1")
  lazy val sequencer1_ : SequencerNodeReference = s("sequencer1")
  lazy val sequencer2: LocalSequencerNodeReference = ls("sequencer2")
  lazy val sequencer3: LocalSequencerNodeReference = ls("sequencer3")
  lazy val sequencer4: LocalSequencerNodeReference = ls("sequencer4")

  lazy val mediator1: LocalMediatorReference = lm("mediator1")
  lazy val mediator2: LocalMediatorReference = lm("mediator2")
  lazy val mediator3: LocalMediatorReference = lm("mediator3")
  lazy val mediator4: LocalMediatorReference = lm("mediator4")

  // Remote
  lazy val remoteSequencer1: RemoteSequencerNodeReference = rs("sequencer1")
  lazy val remoteSequencer2: RemoteSequencerNodeReference = rs("sequencer2")
  lazy val remoteSequencer3: RemoteSequencerNodeReference = rs("sequencer3")
  lazy val remoteSequencer4: RemoteSequencerNodeReference = rs("sequencer4")

  lazy val remoteMediator1: RemoteMediatorReference = rm("mediator1")
  lazy val remoteMediator2: RemoteMediatorReference = rm("mediator2")
  lazy val remoteMediator3: RemoteMediatorReference = rm("mediator3")
  lazy val remoteMediator4: RemoteMediatorReference = rm("mediator4")

  lazy val remoteParticipant1: RemoteParticipantReference = rp("participant1")
  lazy val remoteParticipant2: RemoteParticipantReference = rp("participant2")
  lazy val remoteParticipant3: RemoteParticipantReference = rp("participant3")
  lazy val remoteParticipant4: RemoteParticipantReference = rp("participant4")
  lazy val remoteParticipant5: RemoteParticipantReference = rp("participant5")
  lazy val remoteParticipant6: RemoteParticipantReference = rp("participant6")
  lazy val remoteParticipant7: RemoteParticipantReference = rp("participant7")
  lazy val remoteParticipant8: RemoteParticipantReference = rp("participant8")
}
