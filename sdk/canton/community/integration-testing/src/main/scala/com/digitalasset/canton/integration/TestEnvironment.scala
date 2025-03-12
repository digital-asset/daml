// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleEnvironmentTestHelpers,
  ConsoleMacros,
}
import org.apache.pekko.actor.ActorSystem

/** Type including all environment macros and utilities to appear as you're using canton console */
trait TestEnvironment
    extends ConsoleEnvironmentTestHelpers[ConsoleEnvironment]
    with ConsoleMacros
    with CommonTestAliases[ConsoleEnvironment]
    with ConsoleEnvironment.Implicits {
  this: ConsoleEnvironment =>
  val actualConfig: CantonConfig

  implicit val executionContext: ExecutionContextIdlenessExecutorService =
    environment.executionContext
  implicit val actorSystem: ActorSystem = environment.actorSystem
  implicit val executionSequencerFactory: ExecutionSequencerFactory =
    environment.executionSequencerFactory

  def verifyParticipantLapiIntegrity(plugins: Seq[EnvironmentSetupPlugin[_]]): Unit = ()
}
