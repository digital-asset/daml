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
  def lpx(name: String): LocalParticipantReference = participants.local
    .find(_.name == name)
    .getOrElse(sys.error(s"participant [$name] not configured"))

  def rpx(name: String): RemoteParticipantReference =
    participants.remote
      .find(_.name == name)
      .getOrElse(sys.error(s"remote participant [$name] not configured"))

  def px(name: String): ParticipantReference = participants.all
    .find(_.name == name)
    .getOrElse(sys.error(s"neither local nor remote participant [$name] is configured"))

  def sx(name: String): SequencerNodeReference =
    sequencers.all
      .find(_.name == name)
      .getOrElse(sys.error(s"sequencer [$name] not configured"))

  def lsx(name: String): LocalSequencerNodeReference =
    sequencers.local
      .find(_.name == name)
      .getOrElse(sys.error(s"local sequencer [$name] not configured"))

  def rsx(name: String): RemoteSequencerNodeReference =
    sequencers.remote
      .find(_.name == name)
      .getOrElse(sys.error(s"remote sequencer [$name] not configured"))

  def mx(name: String): LocalMediatorReference =
    mediators.local
      .find(_.name == name)
      .getOrElse(sys.error(s"mediator [$name] not configured"))

  def lmx(name: String): LocalMediatorReference =
    mediators.local
      .find(_.name == name)
      .getOrElse(sys.error(s"local mediator [$name] not configured"))

  def rmx(name: String): RemoteMediatorReference =
    mediators.remote
      .find(_.name == name)
      .getOrElse(sys.error(s"remote mediator [$name] not configured"))
}
