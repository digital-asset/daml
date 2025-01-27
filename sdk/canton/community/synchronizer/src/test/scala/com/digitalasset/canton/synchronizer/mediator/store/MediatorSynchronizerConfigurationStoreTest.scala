// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.store

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, SequencerAlias}
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.{AsyncWordSpec, AsyncWordSpecLike}

trait MediatorSynchronizerConfigurationStoreTest {
  this: AsyncWordSpecLike with BaseTest =>

  def mediatorSynchronizerConfigurationStore(
      mkStore: => MediatorSynchronizerConfigurationStore
  ): Unit = {
    "returns nothing for an empty store" in {
      val store = mkStore

      for {
        config <- store.fetchConfiguration
      } yield config shouldBe None
    }.failOnShutdown("Unexpected shutdown.")

    "when set returns set value" in {
      val store = mkStore
      val connection = GrpcSequencerConnection(
        NonEmpty(Seq, Endpoint("sequencer", Port.tryCreate(100))),
        transportSecurity = true,
        None,
        SequencerAlias.Default,
      )
      val originalConfig = MediatorSynchronizerConfiguration(
        DefaultTestIdentities.synchronizerId,
        defaultStaticSynchronizerParameters,
        SequencerConnections.single(connection),
      )

      for {
        _ <- store.saveConfiguration(originalConfig)
        persistedConfig <- store.fetchConfiguration.map(_.value)
      } yield persistedConfig shouldBe originalConfig
    }.failOnShutdown("Unexpected shutdown.")

    "supports updating the config" in {
      val store = mkStore
      val defaultParams = defaultStaticSynchronizerParameters
      val connection = GrpcSequencerConnection(
        NonEmpty(
          Seq,
          Endpoint("sequencer", Port.tryCreate(200)),
          Endpoint("sequencer", Port.tryCreate(300)),
        ),
        transportSecurity = true,
        None,
        SequencerAlias.Default,
      )
      val originalConfig = MediatorSynchronizerConfiguration(
        DefaultTestIdentities.synchronizerId,
        defaultParams,
        SequencerConnections.single(connection),
      )

      val updatedConfig = originalConfig
        .focus(_.synchronizerParameters)
        .replace(
          BaseTest.defaultStaticSynchronizerParameters
        )

      for {
        _ <- store.saveConfiguration(originalConfig)
        persistedConfig1 <- store.fetchConfiguration.map(_.value)
        _ = persistedConfig1 shouldBe originalConfig
        _ <- store.saveConfiguration(updatedConfig)
        persistedConfig2 <- store.fetchConfiguration.map(_.value)
      } yield persistedConfig2 shouldBe updatedConfig

    }.failOnShutdown("Unexpected shutdown.")
  }
}

class MediatorSynchronizerConfigurationStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with MediatorSynchronizerConfigurationStoreTest {

  behave like mediatorSynchronizerConfigurationStore(
    new InMemoryMediatorSynchronizerConfigurationStore()
  )
}

trait DbMediatorSynchronizerConfigurationStoreTest
    extends AsyncWordSpec
    with BaseTest
    with MediatorSynchronizerConfigurationStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table mediator_synchronizer_configuration"
      ),
      functionFullName,
    )
  }

  behave like mediatorSynchronizerConfigurationStore(
    new DbMediatorSynchronizerConfigurationStore(storage, timeouts, loggerFactory)
  )

}

class MediatorSynchronizerConfigurationStoreTestPostgres
    extends DbMediatorSynchronizerConfigurationStoreTest
    with PostgresTest

class MediatorSynchronizerConfigurationStoreTestH2
    extends DbMediatorSynchronizerConfigurationStoreTest
    with H2Test
