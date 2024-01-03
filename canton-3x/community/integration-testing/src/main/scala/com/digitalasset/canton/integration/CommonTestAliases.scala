// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleEnvironmentTestHelpers,
  LocalMediatorReferenceX,
  LocalParticipantReference,
  LocalParticipantReferenceX,
  LocalSequencerNodeReferenceX,
  ParticipantReference,
  ParticipantReferenceX,
  RemoteSequencerNodeReferenceX,
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
  lazy val participant1x: LocalParticipantReferenceX = lpx("participant1")
  lazy val participant1x_ : ParticipantReferenceX = px("participant1")
  lazy val participant2x: LocalParticipantReferenceX = lpx("participant2")
  lazy val participant3x: LocalParticipantReferenceX = lpx("participant3")
  lazy val participant4x: LocalParticipantReferenceX = lpx("participant4")
  lazy val participant5x: LocalParticipantReferenceX = lpx("participant5")
  lazy val da: CE#DomainLocalRef = d("da")
  lazy val acme: CE#DomainLocalRef = d("acme")
  lazy val repairDomain: CE#DomainLocalRef = d("repair")

  lazy val sequencer1x: LocalSequencerNodeReferenceX = sx("sequencer1")
  lazy val sequencer2x: LocalSequencerNodeReferenceX = sx("sequencer2")
  lazy val sequencer3x: LocalSequencerNodeReferenceX = sx("sequencer3")
  lazy val sequencer4x: LocalSequencerNodeReferenceX = sx("sequencer4")

  // Remote
  lazy val remoteSequencer1x: RemoteSequencerNodeReferenceX = rsx("sequencer1")

  lazy val mediator1x: LocalMediatorReferenceX = mx("mediator1")
  lazy val mediator2x: LocalMediatorReferenceX = mx("mediator2")
  lazy val mediator3x: LocalMediatorReferenceX = mx("mediator3")
  lazy val mediator4x: LocalMediatorReferenceX = mx("mediator4")

}
