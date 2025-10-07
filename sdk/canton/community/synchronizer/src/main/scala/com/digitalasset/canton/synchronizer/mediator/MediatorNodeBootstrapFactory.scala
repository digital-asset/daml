// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreFactory
import com.digitalasset.canton.environment.NodeFactoryArguments
import com.digitalasset.canton.resource.StorageSingleFactory
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService

trait MediatorNodeBootstrapFactory {

  def create(
      arguments: NodeFactoryArguments[
        MediatorNodeConfig,
        MediatorNodeParameters,
        MediatorMetrics,
      ]
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      executionSequencerFactory: ExecutionSequencerFactory,
      actorSystem: ActorSystem,
  ): Either[String, MediatorNodeBootstrap]
}

object CommunityMediatorNodeBootstrapFactory extends MediatorNodeBootstrapFactory {

  override def create(
      arguments: NodeFactoryArguments[MediatorNodeConfig, MediatorNodeParameters, MediatorMetrics]
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      scheduler: ScheduledExecutorService,
      executionSequencerFactory: ExecutionSequencerFactory,
      actorSystem: ActorSystem,
  ): Either[String, MediatorNodeBootstrap] =
    arguments
      .toCantonNodeBootstrapCommonArguments(
        new StorageSingleFactory(arguments.config.storage),
        new CryptoPrivateStoreFactory(
          arguments.config.crypto.provider,
          arguments.config.crypto.kms,
          arguments.config.parameters.caching.kmsMetadataCache,
          arguments.config.crypto.privateKeyStore,
          replicaManager = None,
          arguments.futureSupervisor,
          arguments.clock,
          arguments.executionContext,
        ),
      )
      .map { bootstrapArguments =>
        new MediatorNodeBootstrap(
          bootstrapArguments,
          new CommunityMediatorReplicaManager(
            arguments.parameters.processingTimeouts,
            arguments.loggerFactory,
          ),
        )
      }

}
