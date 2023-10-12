// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.topology.*

/** Utilities for accessing the console environment
  */
trait ConsoleEnvironmentTestHelpers[+CE <: ConsoleEnvironment] { this: CE =>

  lazy val testConsoleOutput: TestConsoleOutput = consoleOutput match {
    case testConsoleOutput: TestConsoleOutput => testConsoleOutput
    case _: ConsoleOutput =>
      throw new IllegalStateException(
        s"Unable to retrieve testConsoleOutput, " +
          s"as the console environment is using a console of type ${consoleOutput.getClass.getName}."
      )
  }

  // helpers for creating participant and domain references by name
  // unknown names will throw
  def lp(name: String): LocalParticipantReference =
    participants.local
      .find(_.name == name)
      .getOrElse(sys.error(s"participant [$name] not configured"))

  def rp(name: String): RemoteParticipantReference =
    participants.remote
      .find(_.name == name)
      .getOrElse(sys.error(s"remote participant [$name] not configured"))

  def p(name: String): ParticipantReference = participants.all
    .find(_.name == name)
    .getOrElse(sys.error(s"neither local nor remote participant [$name] is configured"))

  def px(name: String): LocalParticipantReferenceX = participantsX.local
    .find(_.name == name)
    .getOrElse(sys.error(s"neither local nor remote participant x [$name] is configured"))

  def d(name: String): CE#DomainLocalRef =
    domains.local
      .find(_.name == name)
      .getOrElse(sys.error(s"domain [$name] not configured"))

  def rd(name: String): CE#DomainRemoteRef =
    domains.remote
      .find(_.name == name)
      .getOrElse(sys.error(s"remote domain [$name] not configured"))

  def mediatorIdForDomain(domain: String): MediatorId = MediatorId(d(domain).id)
}
