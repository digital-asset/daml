// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.BaseTest.testedReleaseProtocolVersion
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.{CachingConfigs, CantonConfig, CryptoConfig}
import com.digitalasset.canton.console.commands.GlobalSecretKeyAdministration
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleEnvironmentTestHelpers,
  ConsoleMacros,
  InstanceReference,
  LocalInstanceReference,
}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import org.apache.pekko.actor.ActorSystem

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Await

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

  private lazy val storage =
    new MemoryStorage(loggerFactory, environmentTimeouts)

  private lazy val cryptoET: EitherT[FutureUnlessShutdown, String, Crypto] = Crypto
    .create(
      CryptoConfig(),
      CachingConfigs.defaultKmsMetadataCache,
      CachingConfigs.defaultSessionEncryptionKeyCacheConfig,
      CachingConfigs.defaultPublicKeyConversionCache,
      storage,
      Option.empty[ReplicaManager],
      testedReleaseProtocolVersion,
      FutureSupervisor.Noop,
      environment.clock,
      executionContext,
      environmentTimeouts,
      loggerFactory,
      NoReportingTracerProvider,
    )(executionContext, TraceContext.empty)

  private lazy val crypto: Crypto =
    Await
      .result(cryptoET.value, environmentTimeouts.unbounded.duration)
      .onShutdown(raiseError("Cannot create Crypto during shutdown"))
      .valueOr(err => raiseError(s"Cannot create Crypto: $err"))

  override private[canton] def tryGlobalCrypto: Crypto = crypto

  override private[canton] def global_secret: GlobalSecretKeyAdministration = global_secret_
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
