// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleEnvironmentTestHelpers,
  ConsoleMacros,
  InstanceReference,
  LocalInstanceReference,
}
import org.apache.pekko.actor.ActorSystem

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/** Type including all environment macros and utilities to appear as you're using canton console */
trait TestEnvironment
    extends ConsoleEnvironmentTestHelpers
    with ConsoleMacros
    with ConsoleEnvironment.Implicits
    with EnvironmentTestHelpers
    with CommonTestAliases {
  this: ConsoleEnvironment =>

  implicit val executionContext: ExecutionContextIdlenessExecutorService =
    environment.executionContext
  implicit val actorSystem: ActorSystem = environment.actorSystem
  implicit val executionSequencerFactory: ExecutionSequencerFactory =
    environment.executionSequencerFactory

  def actualConfig: CantonConfig = environment.config

}

trait EnvironmentTestHelpers {
  this: ConsoleEnvironment =>

  def n(
      name: String
  ): LocalInstanceReference =
    nodes.local
      .find(_.name == name)
      .getOrElse(sys.error(s"node [$name] not configured"))

  val initializedSynchronizers: mutable.Map[SynchronizerAlias, InitializedSynchronizer] = TrieMap()

  def runOnAllInitializedSynchronizersForAllOwners(
      run: (InstanceReference, InitializedSynchronizer) => Unit,
      topologyAwaitIdle: Boolean = true,
  ): Unit =
    IntegrationTestUtilities.runOnAllInitializedSynchronizersForAllOwners(
      initializedSynchronizers.toMap,
      run,
      topologyAwaitIdle,
    )
}
