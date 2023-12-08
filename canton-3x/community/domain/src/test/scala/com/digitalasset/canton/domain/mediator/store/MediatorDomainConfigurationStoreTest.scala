// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.{BaseTest, SequencerAlias}
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.{AsyncWordSpec, AsyncWordSpecLike}

import scala.concurrent.Future

trait MediatorDomainConfigurationStoreTest {
  this: AsyncWordSpecLike with BaseTest =>

  def mediatorDomainConfigurationStore(mkStore: => MediatorDomainConfigurationStore): Unit = {
    "returns nothing for an empty store" in {
      val store = mkStore

      for {
        config <- valueOrFail(store.fetchConfiguration)("fetchConfiguration")
      } yield config shouldBe None
    }

    "when set returns set value" in {
      val store = mkStore
      val connection = GrpcSequencerConnection(
        NonEmpty(Seq, Endpoint("sequencer", Port.tryCreate(100))),
        true,
        None,
        SequencerAlias.Default,
      )
      val originalConfig = MediatorDomainConfiguration(
        Fingerprint.tryCreate("fingerprint"),
        DefaultTestIdentities.domainId,
        defaultStaticDomainParameters,
        SequencerConnections.single(connection),
      )

      for {
        _ <- valueOrFail(store.saveConfiguration(originalConfig))("saveConfiguration")
        persistedConfig <- valueOrFail(store.fetchConfiguration)("fetchConfiguration").map(_.value)
      } yield persistedConfig shouldBe originalConfig
    }

    "supports updating the config" in {
      val store = mkStore
      val defaultParams = defaultStaticDomainParameters
      val connection = GrpcSequencerConnection(
        NonEmpty(
          Seq,
          Endpoint("sequencer", Port.tryCreate(200)),
          Endpoint("sequencer", Port.tryCreate(300)),
        ),
        true,
        None,
        SequencerAlias.Default,
      )
      val originalConfig = MediatorDomainConfiguration(
        Fingerprint.tryCreate("fingerprint"),
        DefaultTestIdentities.domainId,
        defaultParams,
        SequencerConnections.single(connection),
      )

      val updatedConfig = originalConfig
        .focus(_.domainParameters)
        .replace(
          BaseTest.defaultStaticDomainParametersWith(
            uniqueContractKeys = true
          )
        )

      for {
        _ <- valueOrFail(store.saveConfiguration(originalConfig))("save original config")
        persistedConfig1 <- valueOrFail(store.fetchConfiguration)("fetch original config")
          .map(_.value)
        _ = persistedConfig1 shouldBe originalConfig
        _ <- valueOrFail(store.saveConfiguration(updatedConfig))("save updated config")
        persistedConfig2 <- valueOrFail(store.fetchConfiguration)("fetch updated config")
          .map(_.value)
      } yield persistedConfig2 shouldBe updatedConfig

    }
  }
}

class MediatorDomainConfigurationStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with MediatorDomainConfigurationStoreTest {

  behave like mediatorDomainConfigurationStore(new InMemoryMediatorDomainConfigurationStore())
}

trait DbMediatorDomainConfigurationStoreTest
    extends AsyncWordSpec
    with BaseTest
    with MediatorDomainConfigurationStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table mediator_domain_configuration"
      ),
      functionFullName,
    )
  }

  behave like mediatorDomainConfigurationStore(
    new DbMediatorDomainConfigurationStore(storage, timeouts, loggerFactory)
  )

}

class MediatorDomainConfigurationStoreTestPostgres
    extends DbMediatorDomainConfigurationStoreTest
    with PostgresTest

class MediatorDomainConfigurationStoreTestH2
    extends DbMediatorDomainConfigurationStoreTest
    with H2Test
