// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  Active,
  AtMostOnePhysicalActive,
  ConfigAlreadyExists,
  Inactive,
  MissingConfigForSynchronizer,
  NoActiveSynchronizer,
  UnknownAlias,
}
import com.digitalasset.canton.participant.store.memory.InMemoryRegisteredSynchronizersStore
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasManager,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  SequencerAlias,
  SynchronizerAlias,
}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

trait SynchronizerConnectionConfigStoreTest extends FailOnShutdown {
  this: AsyncWordSpec with BaseTest with HasExecutionContext =>

  private val uid = DefaultTestIdentities.uid
  private val synchronizerId =
    PhysicalSynchronizerId(SynchronizerId(uid), testedProtocolVersion)
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
    Some(synchronizerId.logical),
    42,
    Some(NonNegativeFiniteDuration.tryOfSeconds(1)),
    Some(NonNegativeFiniteDuration.tryOfSeconds(5)),
    SynchronizerTimeTrackerConfig(),
  )

  protected lazy val synchronizers = {
    val store = new InMemoryRegisteredSynchronizersStore(loggerFactory)
    store.addMapping(alias, synchronizerId.logical).futureValueUS

    store
  }
  protected lazy val aliasManager =
    SynchronizerAliasManager.create(synchronizers, loggerFactory).futureValueUS

  def synchronizerConnectionConfigStore(
      mk: => FutureUnlessShutdown[SynchronizerConnectionConfigStore]
  ): Unit = {
    "when storing connection configs" should {

      "be able to store and retrieve a config successfully" in {
        for {
          sut <- mk
          _ <- valueOrFail(sut.put(config, Active, KnownPhysicalSynchronizerId(synchronizerId)))(
            "failed to add config to synchronizer config store"
          )
          retrievedConfig <- FutureUnlessShutdown.pure(
            valueOrFail(sut.get(alias, KnownPhysicalSynchronizerId(synchronizerId)))(
              "failed to retrieve config from synchronizer config store"
            )
          )
        } yield retrievedConfig.config shouldBe config
      }

      "store the same config twice for idempotency" in {
        for {
          sut <- mk
          _ <- sut
            .put(config, Active, KnownPhysicalSynchronizerId(synchronizerId))
            .valueOrFail("first store of config")
          _ <- sut
            .put(config, Active, KnownPhysicalSynchronizerId(synchronizerId))
            .valueOrFail("second store of config")
        } yield succeed
      }

      "return error if synchronizer alias config already exists with a different value" in {
        for {
          sut <- mk
          _ <- sut
            .put(config, Active, KnownPhysicalSynchronizerId(synchronizerId))
            .valueOrFail("first store of config")
          result <- sut
            .put(
              config.copy(manualConnect = true),
              Active,
              KnownPhysicalSynchronizerId(synchronizerId),
            )
            .value
        } yield {
          result shouldBe Left(
            ConfigAlreadyExists(alias, KnownPhysicalSynchronizerId(synchronizerId))
          )
        }
      }

      "return error if config being retrieved does not exist" in {
        for {
          sut <- mk
        } yield {
          sut.get(alias, UnknownPhysicalSynchronizerId) shouldBe Left(
            MissingConfigForSynchronizer(alias, UnknownPhysicalSynchronizerId)
          )
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
          _ <- valueOrFail(sut.put(config, Active, KnownPhysicalSynchronizerId(synchronizerId)))(
            "failed to add config to synchronizer config store"
          )
          _ <- valueOrFail(sut.replace(KnownPhysicalSynchronizerId(synchronizerId), secondConfig))(
            "failed to replace config in config store"
          )
          retrievedConfig <- FutureUnlessShutdown.pure(
            valueOrFail(sut.get(alias, KnownPhysicalSynchronizerId(synchronizerId)))(
              "failed to retrieve config from synchronizer config store"
            )
          )
        } yield retrievedConfig.config shouldBe secondConfig
      }

      "return error if replaced config does not exist" in {
        for {
          sut <- mk
          result <- sut.replace(KnownPhysicalSynchronizerId(synchronizerId), config).value
        } yield result shouldBe Left(
          MissingConfigForSynchronizer(alias, KnownPhysicalSynchronizerId(synchronizerId))
        )
      }

      "be able to retrieve all configs" in {
        val secondConfig = config.copy(synchronizerAlias = SynchronizerAlias.tryCreate("another"))
        for {
          sut <- mk
          _ <- valueOrFail(sut.put(config, Active, KnownPhysicalSynchronizerId(synchronizerId)))(
            "failed to add config to synchronizer config store"
          )
          _ <- valueOrFail(
            sut.put(secondConfig, Active, KnownPhysicalSynchronizerId(synchronizerId))
          )(
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
          _ <- sut
            .put(config, Active, KnownPhysicalSynchronizerId(synchronizerId))
            .valueOrFail("put")
          _ <- sut.refreshCache()
          fetchedConfig = valueOrFail(
            sut.get(config.synchronizerAlias, KnownPhysicalSynchronizerId(synchronizerId))
          )("get")
        } yield fetchedConfig.config shouldBe config
      }
    }

    "support of several physical synchronizers of the same logical instance" should {
      val daId = SynchronizerId(UniqueIdentifier.tryCreate("da", DefaultTestIdentities.namespace))
      val daStable = PhysicalSynchronizerId(daId, ProtocolVersion.latest)
      val daBeta = PhysicalSynchronizerId(daId, ProtocolVersion.parseUnchecked(3444).value)
      val daDev = PhysicalSynchronizerId(daId, ProtocolVersion.dev)
      val daName = SynchronizerAlias.tryCreate("da")

      val acmeId =
        SynchronizerId(UniqueIdentifier.tryCreate("acme", DefaultTestIdentities.namespace))
      val acmeStable = PhysicalSynchronizerId(acmeId, ProtocolVersion.latest)
      val acmeName = SynchronizerAlias.tryCreate("acme")

      def getConfig(id: PhysicalSynchronizerId, manualConnect: Boolean = false) = {
        val alias = SynchronizerAlias.tryCreate(id.logical.uid.identifier.str)

        SynchronizerConnectionConfig(
          alias,
          SequencerConnections.single(connection),
          manualConnect = manualConnect,
          None,
          id.suffix.length,
          Some(NonNegativeFiniteDuration.tryOfSeconds(1)),
          Some(NonNegativeFiniteDuration.tryOfSeconds(5)),
          SynchronizerTimeTrackerConfig(),
        )
      }

      def getStoredSynchronizerConnectionConfig(psid: PhysicalSynchronizerId) =
        StoredSynchronizerConnectionConfig(
          getConfig(psid),
          Active,
          KnownPhysicalSynchronizerId(psid),
        )

      "work (simple case)" in {
        for {
          sut <- mk

          _ <- sut
            .put(getConfig(daStable), Active, KnownPhysicalSynchronizerId(daStable))
            .valueOrFail("put")
          _ <- sut
            .put(getConfig(daDev), Active, KnownPhysicalSynchronizerId(daDev))
            .valueOrFail("put")
          _ <- sut
            .put(getConfig(daBeta), Active, UnknownPhysicalSynchronizerId)
            .valueOrFail("put") // no physical id known
          _ = sut
            .get(config.synchronizerAlias, KnownPhysicalSynchronizerId(daStable))
            .value shouldBe getStoredSynchronizerConnectionConfig(daStable)

          _ = sut
            .get(config.synchronizerAlias, KnownPhysicalSynchronizerId(daDev))
            .value shouldBe getStoredSynchronizerConnectionConfig(daDev)

          _ = sut
            .get(config.synchronizerAlias, UnknownPhysicalSynchronizerId)
            .value shouldBe StoredSynchronizerConnectionConfig(
            getConfig(daBeta),
            Active,
            UnknownPhysicalSynchronizerId,
          )
        } yield succeed
      }

      "store the same config twice for idempotency" in {
        for {
          sut <- mk

          _ <- sut
            .put(getConfig(daStable, true), Active, KnownPhysicalSynchronizerId(daStable))
            .valueOrFail("put (first)")
          _ <- sut
            .put(getConfig(daStable, true), Active, KnownPhysicalSynchronizerId(daStable))
            .valueOrFail("put (second)")

          // different config, should be rejected
          resStable <- sut
            .put(getConfig(daStable, false), Active, KnownPhysicalSynchronizerId(daStable))
            .value
          _ = resStable shouldBe Left(
            ConfigAlreadyExists(daName, KnownPhysicalSynchronizerId(daStable))
          )

        } yield succeed
      }

      "allow to store physical synchronizer id when it is known" in {
        for {
          sut <- mk

          _ <- sut
            .put(getConfig(daStable), Active, UnknownPhysicalSynchronizerId)
            .valueOrFail("put") // no physical id known

          _ <- sut.setPhysicalSynchronizerId(daName, daStable).valueOrFail("set psid (1st)")
          // idempotency
          _ <- sut.setPhysicalSynchronizerId(daName, daStable).valueOrFail("set psid (2nd)")
          _ = sut
            .getActive(daName, singleExpected = true)
            .value
            .configuredPSId shouldBe KnownPhysicalSynchronizerId(daStable)

          unknownAlias <- sut.setPhysicalSynchronizerId(acmeName, acmeStable).value
          _ = unknownAlias.left.value shouldBe MissingConfigForSynchronizer(
            acmeName,
            UnknownPhysicalSynchronizerId,
          )
        } yield succeed
      }

      "allow to replace configs" in {
        for {
          sut <- mk

          _ <- sut
            .put(getConfig(daStable), Active, UnknownPhysicalSynchronizerId)
            .valueOrFail("put") // no physical id known
          _ <- sut
            .put(getConfig(daDev), Active, KnownPhysicalSynchronizerId(daDev))
            .valueOrFail("put") // no physical id known

          _ = sut
            .get(daName, UnknownPhysicalSynchronizerId)
            .value
            .config
            .manualConnect shouldBe false
          _ = sut
            .get(daName, KnownPhysicalSynchronizerId(daDev))
            .value
            .config
            .manualConnect shouldBe false

          _ <- sut
            .replace(UnknownPhysicalSynchronizerId, getConfig(daDev, manualConnect = true))
            .valueOrFail("update (da, UnknownPhysicalSynchronizerId) -> true")
          _ = sut
            .get(daName, UnknownPhysicalSynchronizerId)
            .value
            .config
            .manualConnect shouldBe true
          _ = sut
            .get(daName, KnownPhysicalSynchronizerId(daDev))
            .value
            .config
            .manualConnect shouldBe false

          _ <- sut
            .replace(UnknownPhysicalSynchronizerId, getConfig(daDev, manualConnect = false))
            .valueOrFail("update (da, UnknownPhysicalSynchronizerId) -> false")
          _ = sut
            .get(daName, UnknownPhysicalSynchronizerId)
            .value
            .config
            .manualConnect shouldBe false
          _ = sut
            .get(daName, KnownPhysicalSynchronizerId(daDev))
            .value
            .config
            .manualConnect shouldBe false

          _ <- sut
            .replace(KnownPhysicalSynchronizerId(daDev), getConfig(daDev, manualConnect = true))
            .valueOrFail("update (da, daDev) -> true")
          _ = sut
            .get(daName, UnknownPhysicalSynchronizerId)
            .value
            .config
            .manualConnect shouldBe false
          _ = sut
            .get(daName, KnownPhysicalSynchronizerId(daDev))
            .value
            .config
            .manualConnect shouldBe true

        } yield succeed
      }

      "getActive return active synchronizer" in {
        for {
          sut <- mk

          unknown = sut.getActive(daName, singleExpected = false)
          _ = unknown.left.value shouldBe UnknownAlias(daName)

          _ <- sut
            .put(getConfig(daStable), Inactive, UnknownPhysicalSynchronizerId)
            .valueOrFail("put daStable")
          _ = sut
            .getActive(daName, singleExpected = false)
            .left
            .value shouldBe NoActiveSynchronizer(daName)

          _ <- sut
            .setStatus(daName, UnknownPhysicalSynchronizerId, Active)
            .valueOrFail("set active")
          _ = sut
            .getActive(daName, singleExpected = true)
            .value shouldBe StoredSynchronizerConnectionConfig(
            getConfig(daStable),
            Active,
            UnknownPhysicalSynchronizerId,
          )
          _ = sut
            .getActive(daName, singleExpected = false)
            .value shouldBe StoredSynchronizerConnectionConfig(
            getConfig(daStable),
            Active,
            UnknownPhysicalSynchronizerId,
          )

          _ <- sut
            .put(getConfig(daDev), Active, KnownPhysicalSynchronizerId(daDev))
            .valueOrFail("put daDev")

          _ = sut
            .getActive(daName, singleExpected = false)
            .value shouldBe StoredSynchronizerConnectionConfig(
            getConfig(daDev),
            Active,
            KnownPhysicalSynchronizerId(daDev), // defined id wins
          )
          _ = sut
            .getActive(daName, singleExpected = true)
            .left
            .value shouldBe AtMostOnePhysicalActive(
            daName,
            Set(UnknownPhysicalSynchronizerId, KnownPhysicalSynchronizerId(daDev)),
          )

        } yield succeed
      }
    }
  }
}
