// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

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

  // helpers for creating participant, sequencer, and mediator references by name
  // unknown names will throw
  def lpx(name: String): LocalParticipantReferenceX = participantsX.local
    .find(_.name == name)
    .getOrElse(sys.error(s"participant x [$name] not configured"))

  def rpx(name: String): RemoteParticipantReferenceX =
    participantsX.remote
      .find(_.name == name)
      .getOrElse(sys.error(s"remote participant [$name] not configured"))

  def px(name: String): ParticipantReferenceX = participantsX.all
    .find(_.name == name)
    .getOrElse(sys.error(s"neither local nor remote participant x [$name] is configured"))

  def sx(name: String): SequencerNodeReferenceX =
    sequencersX.all
      .find(_.name == name)
      .getOrElse(sys.error(s"sequencer-x [$name] not configured"))

  def lsx(name: String): LocalSequencerNodeReferenceX =
    sequencersX.local
      .find(_.name == name)
      .getOrElse(sys.error(s"local sequencer-x [$name] not configured"))

  def rsx(name: String): RemoteSequencerNodeReferenceX =
    sequencersX.remote
      .find(_.name == name)
      .getOrElse(sys.error(s"remote sequencer-x [$name] not configured"))

  def mx(name: String): LocalMediatorReferenceX =
    mediatorsX.local
      .find(_.name == name)
      .getOrElse(sys.error(s"mediator-x [$name] not configured"))

  def lmx(name: String): LocalMediatorReferenceX =
    mediatorsX.local
      .find(_.name == name)
      .getOrElse(sys.error(s"local mediator-x [$name] not configured"))

  def rmx(name: String): RemoteMediatorReferenceX =
    mediatorsX.remote
      .find(_.name == name)
      .getOrElse(sys.error(s"remote mediator-x [$name] not configured"))
}
