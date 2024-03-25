// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleEnvironmentTestHelpers,
  ConsoleMacros,
}
import com.digitalasset.canton.environment.Environment
import org.apache.pekko.actor.ActorSystem

/** Type including all environment macros and utilities to appear as you're using canton console */
trait TestEnvironment[+E <: Environment]
    extends ConsoleEnvironmentTestHelpers[E#Console]
    with ConsoleMacros
    with CommonTestAliases[E#Console]
    with ConsoleEnvironment.Implicits {
  this: E#Console =>
  val actualConfig: E#Config

  implicit val executionContext: ExecutionContextIdlenessExecutorService =
    environment.executionContext
  implicit val actorSystem: ActorSystem = environment.actorSystem
  implicit val executionSequencerFactory: ExecutionSequencerFactory =
    environment.executionSequencerFactory
}
