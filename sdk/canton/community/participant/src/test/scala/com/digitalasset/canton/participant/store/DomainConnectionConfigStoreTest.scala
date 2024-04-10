// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.DomainTimeTrackerConfig
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore.{
  AlreadyAddedForAlias,
  MissingConfigForAlias,
}
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.{BaseTest, DomainAlias, SequencerAlias}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DomainConnectionConfigStoreTest {
  this: AsyncWordSpec with BaseTest =>

  private val uid = DefaultTestIdentities.uid
  private val domainId = DomainId(uid)
  private val alias = DomainAlias.tryCreate("da")
  private val connection = GrpcSequencerConnection(
    NonEmpty(Seq, Endpoint("host1", Port.tryCreate(500)), Endpoint("host2", Port.tryCreate(600))),
    false,
    Some(ByteString.copyFrom("stuff".getBytes)),
    SequencerAlias.Default,
  )
  private val config = DomainConnectionConfig(
    alias,
    SequencerConnections.single(connection),
    manualConnect = false,
    Some(domainId),
    42,
    Some(NonNegativeFiniteDuration.tryOfSeconds(1)),
    Some(NonNegativeFiniteDuration.tryOfSeconds(5)),
    DomainTimeTrackerConfig(),
  )

  def domainConnectionConfigStore(mk: => Future[DomainConnectionConfigStore]): Unit = {
    val status = DomainConnectionConfigStore.Active
    "when storing connection configs" should {

      "be able to store and retrieve a config successfully" in {
        for {
          sut <- mk
          _ <- valueOrFail(sut.put(config, status))("failed to add config to domain config store")
          retrievedConfig <- Future.successful(
            valueOrFail(sut.get(alias))("failed to retrieve config from domain config store")
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
      "return error if domain alias config already exists with a different value" in {
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
          false,
          None,
          SequencerAlias.Default,
        )
        val secondConfig = DomainConnectionConfig(
          alias,
          SequencerConnections.single(connection),
          manualConnect = true,
          None,
          99,
          None,
          None,
          DomainTimeTrackerConfig(),
        )
        for {
          sut <- mk
          _ <- valueOrFail(sut.put(config, status))("failed to add config to domain config store")
          _ <- valueOrFail(sut.replace(secondConfig))("failed to replace config in config store")
          retrievedConfig <- Future.successful(
            valueOrFail(sut.get(alias))("failed to retrieve config from domain config store")
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
        val secondConfig = config.copy(domain = DomainAlias.tryCreate("another"))
        for {
          sut <- mk
          _ <- valueOrFail(sut.put(config, status))("failed to add config to domain config store")
          _ <- valueOrFail(sut.put(secondConfig, status))(
            "failed to add second config to domain config store"
          )
          result = sut.getAll()
        } yield result.map(_.config) should contain.allOf(config, secondConfig)
      }
    }

    "resetting the cache" should {
      "refresh with same values" in {
        for {
          sut <- mk
          _ <- valueOrFail(sut.put(config, status))("put")
          _ <- sut.refreshCache()
          fetchedConfig = valueOrFail(sut.get(config.domain))("get")
        } yield fetchedConfig.config shouldBe config
      }
    }
  }
}
