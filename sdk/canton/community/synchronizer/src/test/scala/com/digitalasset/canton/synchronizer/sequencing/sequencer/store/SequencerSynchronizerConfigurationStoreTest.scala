// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.{AsyncWordSpec, AsyncWordSpecLike}

trait SequencerSynchronizerConfigurationStoreTest {
  this: AsyncWordSpecLike with BaseTest =>

  def synchronizerConfigurationStore(mkStore: => SequencerSynchronizerConfigurationStore): Unit = {
    "returns nothing for an empty store" in {
      val store = mkStore

      for {
        config <- valueOrFail(store.fetchConfiguration)("fetchConfiguration").failOnShutdown
      } yield config shouldBe None
    }

    "when set returns set value" in {
      val store = mkStore
      val originalConfig = SequencerSynchronizerConfiguration(
        DefaultTestIdentities.synchronizerId,
        defaultStaticSynchronizerParameters,
      )

      (for {
        _ <- valueOrFail(store.saveConfiguration(originalConfig))("saveConfiguration")
        persistedConfig <- valueOrFail(store.fetchConfiguration)("fetchConfiguration").map(_.value)
      } yield persistedConfig shouldBe originalConfig).failOnShutdown
    }

    "supports updating the config" in {
      val store = mkStore
      val defaultParams = defaultStaticSynchronizerParameters
      val originalConfig = SequencerSynchronizerConfiguration(
        DefaultTestIdentities.synchronizerId,
        defaultParams,
      )
      val updatedConfig = originalConfig
        .focus(_.synchronizerParameters)
        .replace(
          BaseTest.defaultStaticSynchronizerParameters
        )

      (for {
        _ <- valueOrFail(store.saveConfiguration(originalConfig))("save original config")
        persistedConfig1 <- valueOrFail(store.fetchConfiguration)("fetch original config")
          .map(_.value)
        _ = persistedConfig1 shouldBe originalConfig
        _ <- valueOrFail(store.saveConfiguration(updatedConfig))("save updated config")
        persistedConfig2 <- valueOrFail(store.fetchConfiguration)("fetch updated config")
          .map(_.value)
      } yield persistedConfig2 shouldBe updatedConfig).failOnShutdown

    }
  }
}

class SequencerSynchronizerConfigurationStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with SequencerSynchronizerConfigurationStoreTest {

  behave like synchronizerConfigurationStore(new InMemorySequencerSynchronizerConfigurationStore())
}

trait DbSequencerSynchronizerConfigurationStoreTest
    extends AsyncWordSpec
    with BaseTest
    with SequencerSynchronizerConfigurationStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table sequencer_synchronizer_configuration"
      ),
      functionFullName,
    )
  }

  behave like synchronizerConfigurationStore(
    new DbSequencerSynchronizerConfigurationStore(storage, timeouts, loggerFactory)
  )

}

class SequencerSynchronizerConfigurationStoreTestPostgres
    extends DbSequencerSynchronizerConfigurationStoreTest
    with PostgresTest

class SequencerSynchronizerConfigurationStoreTestH2
    extends DbSequencerSynchronizerConfigurationStoreTest
    with H2Test
