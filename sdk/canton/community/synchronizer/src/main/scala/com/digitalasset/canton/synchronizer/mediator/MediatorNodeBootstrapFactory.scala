// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.environment.NodeFactoryArguments
import com.digitalasset.canton.resource.{DbLockCounters, StorageMultiFactory}
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService

trait MediatorNodeBootstrapFactory {
  def create(
      arguments: NodeFactoryArguments[MediatorNodeConfig, MediatorNodeParameters, MediatorMetrics]
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      executionSequencerFactory: ExecutionSequencerFactory,
      actorSystem: ActorSystem,
  ): Either[String, MediatorNodeBootstrap]
}

object MediatorNodeBootstrapFactoryImpl extends MediatorNodeBootstrapFactory {

  override def create(
      arguments: NodeFactoryArguments[MediatorNodeConfig, MediatorNodeParameters, MediatorMetrics]
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      executionSequencerFactory: ExecutionSequencerFactory,
      actorSystem: ActorSystem,
  ): Either[String, MediatorNodeBootstrap] = {
    val replicaManager =
      new MediatorReplicaManager(
        arguments.parameters.exitOnFatalFailures,
        arguments.parameters.processingTimeouts,
        arguments.loggerFactory,
        arguments.futureSupervisor,
      )

    arguments
      .toCantonNodeBootstrapCommonArguments(
        new StorageMultiFactory(
          arguments.config.storage,
          exitOnFatalFailures = arguments.parameters.exitOnFatalFailures,
          arguments.config.replication,
          () => replicaManager.setActive(),
          () => replicaManager.setPassive(),
          DbLockCounters.MEDIATOR_WRITE,
          DbLockCounters.MEDIATOR_WRITERS,
          arguments.futureSupervisor,
          arguments.loggerFactory,
          Some(() => replicaManager.getSessionContext),
        ),
        Some(replicaManager),
      )
      .map { bootstrapArguments =>
        new MediatorNodeBootstrap(bootstrapArguments, replicaManager) {
          override def onClosed(): Unit = {
            this.replicaManager.close()
            super.onClosed()
          }
        }
      }
  }
}
