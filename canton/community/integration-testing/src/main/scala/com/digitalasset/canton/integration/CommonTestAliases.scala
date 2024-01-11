// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleEnvironmentTestHelpers,
  LocalParticipantReference,
  LocalParticipantReferenceX,
  ParticipantReference,
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
  lazy val participant1x: LocalParticipantReferenceX = px("participant1")
  lazy val participant2x: LocalParticipantReferenceX = px("participant2")
  lazy val participant3x: LocalParticipantReferenceX = px("participant3")
  lazy val participant4x: LocalParticipantReferenceX = px("participant4")
  lazy val da: CE#DomainLocalRef = d("da")
  lazy val acme: CE#DomainLocalRef = d("acme")
  lazy val repairDomain: CE#DomainLocalRef = d("repair")

}
