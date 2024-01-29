// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleEnvironmentTestHelpers,
  LocalMediatorReferenceX,
  LocalParticipantReferenceX,
  LocalSequencerNodeReferenceX,
  ParticipantReferenceX,
  RemoteMediatorReferenceX,
  RemoteSequencerNodeReferenceX,
  SequencerNodeReferenceX,
}

/** Aliases used by our typical single domain and multi domain tests.
  * If a test attempts to use an aliases against an environment without
  * that node configured it will immediately throw.
  */
trait CommonTestAliases[+CE <: ConsoleEnvironment] {
  this: ConsoleEnvironmentTestHelpers[CE] =>
  lazy val participant1x: LocalParticipantReferenceX = lpx("participant1")
  lazy val participant1x_ : ParticipantReferenceX = px("participant1")
  lazy val participant2x: LocalParticipantReferenceX = lpx("participant2")
  lazy val participant3x: LocalParticipantReferenceX = lpx("participant3")
  lazy val participant4x: LocalParticipantReferenceX = lpx("participant4")
  lazy val participant5x: LocalParticipantReferenceX = lpx("participant5")

  lazy val sequencer1x: LocalSequencerNodeReferenceX = lsx("sequencer1")
  lazy val sequencer1x_ : SequencerNodeReferenceX = sx("sequencer1")
  lazy val sequencer2x: LocalSequencerNodeReferenceX = lsx("sequencer2")
  lazy val sequencer3x: LocalSequencerNodeReferenceX = lsx("sequencer3")
  lazy val sequencer4x: LocalSequencerNodeReferenceX = lsx("sequencer4")

  // Remote
  lazy val remoteSequencer1x: RemoteSequencerNodeReferenceX = rsx("sequencer1")
  lazy val remoteSequencer2x: RemoteSequencerNodeReferenceX = rsx("sequencer2")
  lazy val remoteSequencer3x: RemoteSequencerNodeReferenceX = rsx("sequencer3")
  lazy val remoteMediator1x: RemoteMediatorReferenceX = rmx("mediator1")

  lazy val mediator1x: LocalMediatorReferenceX = lmx("mediator1")
  lazy val mediator2x: LocalMediatorReferenceX = lmx("mediator2")
  lazy val mediator3x: LocalMediatorReferenceX = lmx("mediator3")
  lazy val mediator4x: LocalMediatorReferenceX = lmx("mediator4")

}
