// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.crypto.kms.CommunityKmsFactory
import com.digitalasset.canton.crypto.store.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.environment.NodeFactoryArguments
import com.digitalasset.canton.resource.CommunityStorageFactory
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

object CommunitySequencerNodeBootstrapFactory extends SequencerNodeBootstrapFactory {

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
  ): Either[String, SequencerNodeBootstrap] =
    arguments
      .toCantonNodeBootstrapCommonArguments(
        new CommunityStorageFactory(arguments.config.storage),
        new CommunityCryptoPrivateStoreFactory(
          arguments.config.crypto.provider,
          arguments.config.crypto.kms,
          CommunityKmsFactory,
          arguments.config.parameters.caching.kmsMetadataCache,
          arguments.config.crypto.privateKeyStore,
          arguments.parameters.nonStandardConfig,
          arguments.futureSupervisor,
          arguments.clock,
          arguments.executionContext,
        ),
        CommunityKmsFactory,
      )
      .map { bootstrapCommonArguments =>
        new SequencerNodeBootstrap(bootstrapCommonArguments, CommunitySequencerFactory)
      }

}
