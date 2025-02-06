// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.{BaseTest, FailOnShutdown, SequencerAlias, SynchronizerAlias}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

trait SynchronizerConnectionConfigStoreTest extends FailOnShutdown {
  this: AsyncWordSpec with BaseTest =>

  private val uid = DefaultTestIdentities.uid
  private val synchronizerId = SynchronizerId(uid)
  private val alias = SynchronizerAlias.tryCreate("da")
  private val connection = GrpcSequencerConnection(
    NonEmpty(Seq, Endpoint("host1", Port.tryCreate(500)), Endpoint("host2", Port.tryCreate(600))),
    transportSecurity = false,
    Some(ByteString.copyFrom("stuff".getBytes)),
    SequencerAlias.Default,
  )
  private val config = SynchronizerConnectionConfig(
    alias,
    SequencerConnections.single(connection),
    manualConnect = false,
    Some(synchronizerId),
    42,
    Some(NonNegativeFiniteDuration.tryOfSeconds(1)),
    Some(NonNegativeFiniteDuration.tryOfSeconds(5)),
    SynchronizerTimeTrackerConfig(),
  )

  def synchronizerConnectionConfigStore(
      mk: => FutureUnlessShutdown[SynchronizerConnectionConfigStore]
  ): Unit = {
    val status = SynchronizerConnectionConfigStore.Active
    "when storing connection configs" should {

      "be able to store and retrieve a config successfully" in {
        for {
          sut <- mk
          _ <- valueOrFail(sut.put(config, status))(
            "failed to add config to synchronizer config store"
          )
          retrievedConfig <- FutureUnlessShutdown.pure(
            valueOrFail(sut.get(alias))("failed to retrieve config from synchronizer config store")
          )
        } yield retrievedConfig.config shouldBe config
      }
      "store the same config twice for idempotency" in {
        for {
          sut <- mk
          _ <- sut.put(config, status).valueOrFail("first store of config")
          _ <- sut.put(config, status).valueOrFail("second store of config")
        } yield succeed

      }
      "return error if synchronizer alias config already exists with a different value" in {
        for {
          sut <- mk
          _ <- sut.put(config, status).valueOrFail("first store of config")
          result <- sut.put(config.copy(manualConnect = true), status).value
        } yield {
          result shouldBe Left(AlreadyAddedForAlias(alias))
        }
      }
      "return error if config being retrieved does not exist" in {
        for {
          sut <- mk
        } yield {
          sut.get(alias) shouldBe Left(MissingConfigForAlias(alias))
        }
      }
      "be able to replace a config" in {
        val connection = GrpcSequencerConnection(
          NonEmpty(
            Seq,
            Endpoint("newHost1", Port.tryCreate(500)),
            Endpoint("newHost2", Port.tryCreate(600)),
          ),
          transportSecurity = false,
          None,
          SequencerAlias.Default,
        )
        val secondConfig = SynchronizerConnectionConfig(
          alias,
          SequencerConnections.single(connection),
          manualConnect = true,
          None,
          99,
          None,
          None,
          SynchronizerTimeTrackerConfig(),
        )
        for {
          sut <- mk
          _ <- valueOrFail(sut.put(config, status))(
            "failed to add config to synchronizer config store"
          )
          _ <- valueOrFail(sut.replace(secondConfig))("failed to replace config in config store")
          retrievedConfig <- FutureUnlessShutdown.pure(
            valueOrFail(sut.get(alias))("failed to retrieve config from synchronizer config store")
          )
        } yield retrievedConfig.config shouldBe secondConfig
      }
      "return error if replaced config does not exist" in {
        for {
          sut <- mk
          result <- sut.replace(config).value
        } yield result shouldBe Left(MissingConfigForAlias(alias))
      }
      "be able to retrieve all configs" in {
        val secondConfig = config.copy(synchronizerAlias = SynchronizerAlias.tryCreate("another"))
        for {
          sut <- mk
          _ <- valueOrFail(sut.put(config, status))(
            "failed to add config to synchronizer config store"
          )
          _ <- valueOrFail(sut.put(secondConfig, status))(
            "failed to add second config to synchronizer config store"
          )
          result = sut.getAll()
        } yield result.map(_.config) should contain.allOf(config, secondConfig)
      }
    }

    "resetting the cache" should {
      "refresh with same values" in {
        for {
          sut <- mk
          _ <- sut.put(config, status).valueOrFail("put")
          _ <- sut.refreshCache()
          fetchedConfig = valueOrFail(sut.get(config.synchronizerAlias))("get")
        } yield fetchedConfig.config shouldBe config
      }
    }
  }
}
