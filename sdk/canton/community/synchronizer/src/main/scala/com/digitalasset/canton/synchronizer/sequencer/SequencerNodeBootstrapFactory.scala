// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.environment.{
  CantonNodeBootstrapCommonArguments,
  NodeFactoryArguments,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.{DbLockCounters, StorageFactory, StorageMultiFactory}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.config.{
  SequencerNodeConfig,
  SequencerNodeParameters,
}
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService

trait SequencerNodeBootstrapFactory {

  def create(
      arguments: NodeFactoryArguments[
        SequencerNodeConfig,
        SequencerNodeParameters,
        SequencerMetrics,
      ]
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      actorSystem: ActorSystem,
  ): Either[String, SequencerNodeBootstrap]
}

object SequencerNodeBootstrapFactoryImpl extends SequencerNodeBootstrapFactory {
  override def create(
      arguments: NodeFactoryArguments[
        SequencerNodeConfig,
        SequencerNodeParameters,
        SequencerMetrics,
      ]
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      actorSystem: ActorSystem,
  ): Either[String, SequencerNodeBootstrap] = {
    val storageFactory = new StorageMultiFactory(
      arguments.config.storage,
      exitOnFatalFailures = arguments.parameters.exitOnFatalFailures,
      arguments.config.replication,
      () => FutureUnlessShutdown.unit,
      () => FutureUnlessShutdown.unit,
      DbLockCounters.SEQUENCER_INIT,
      DbLockCounters.SEQUENCER_INIT_WORKER,
      arguments.futureSupervisor,
      arguments.loggerFactory,
      None,
    )

    toNodeCommonArguments(arguments, storageFactory)
      .map(new SequencerNodeBootstrap(_))
  }

  private def toNodeCommonArguments(
      arguments: NodeFactoryArguments[
        SequencerNodeConfig,
        SequencerNodeParameters,
        SequencerMetrics,
      ],
      storageFactory: StorageFactory,
  ): Either[String, CantonNodeBootstrapCommonArguments[
    SequencerNodeConfig,
    SequencerNodeParameters,
    SequencerMetrics,
  ]] =
    arguments
      .toCantonNodeBootstrapCommonArguments(
        storageFactory,
        Option.empty[ReplicaManager],
      )

}
