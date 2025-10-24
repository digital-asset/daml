// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerPredecessor}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  Active,
  AtMostOnePhysicalActive,
  ConfigAlreadyExists,
  ConfigIdentifier,
  Inactive,
  InconsistentLogicalSynchronizerIds,
  InconsistentPredecessorLogicalSynchronizerIds,
  InconsistentSequencerIds,
  MissingConfigForSynchronizer,
  NoActiveSynchronizer,
  SynchronizerIdAlreadyAdded,
  UnknownAlias,
  UnknownPSId,
}
import com.digitalasset.canton.participant.store.memory.InMemoryRegisteredSynchronizersStore
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasManager,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnectionPoolDelays,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.DefaultTestIdentities.namespace
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
  private val psid = SynchronizerId(uid).toPhysical

  private val uid2 = UniqueIdentifier.tryCreate("acme", namespace)
  private val psid2 = SynchronizerId(uid2).toPhysical

  private val daAlias = SynchronizerAlias.tryCreate("da")
  private val acmeAlias = SynchronizerAlias.tryCreate("acme")

  private val sequencerAlias1 = SequencerAlias.tryCreate("sequencer1")
  private val sequencerAlias2 = SequencerAlias.tryCreate("sequencer2")

  private val connection1 = GrpcSequencerConnection(
    NonEmpty(Seq, Endpoint("host1", Port.tryCreate(500)), Endpoint("host2", Port.tryCreate(600))),
    transportSecurity = false,
    Some(ByteString.copyFrom("stuff".getBytes)),
    sequencerAlias1,
    sequencerId = None,
  )

  private val connection2 = GrpcSequencerConnection(
    NonEmpty(Seq, Endpoint("host3", Port.tryCreate(501)), Endpoint("host4", Port.tryCreate(601))),
    transportSecurity = false,
    Some(ByteString.copyFrom("stuff".getBytes)),
    sequencerAlias2,
    sequencerId = None,
  )

  private val config = SynchronizerConnectionConfig(
    daAlias,
    SequencerConnections.single(connection1),
    manualConnect = false,
    Some(psid),
    42,
    Some(NonNegativeFiniteDuration.tryOfSeconds(1)),
    Some(NonNegativeFiniteDuration.tryOfSeconds(5)),
    SynchronizerTimeTrackerConfig(),
  )

  protected lazy val synchronizers = {
    val store = new InMemoryRegisteredSynchronizersStore(loggerFactory)
    store.addMapping(daAlias, psid).futureValueUS

    store
  }
  protected lazy val aliasManager =
    SynchronizerAliasManager.create(synchronizers, loggerFactory).futureValueUS

  private def getConfig(id: PhysicalSynchronizerId, manualConnect: Boolean = false) = {
    val alias = SynchronizerAlias.tryCreate(id.identifier.str)

    SynchronizerConnectionConfig(
      alias,
      SequencerConnections
        .tryMany(
          Seq(connection1, connection2),
          PositiveInt.one,
          NonNegativeInt.zero,
          SubmissionRequestAmplification.NoAmplification,
          SequencerConnectionPoolDelays.default,
        ),
      manualConnect = manualConnect,
      None,
      id.suffix.length,
      Some(NonNegativeFiniteDuration.tryOfSeconds(1)),
      Some(NonNegativeFiniteDuration.tryOfSeconds(5)),
      SynchronizerTimeTrackerConfig(),
    )
  }

  private val daId = SynchronizerId(
    UniqueIdentifier.tryCreate("da", DefaultTestIdentities.namespace)
  )
  private val daStable = PhysicalSynchronizerId(daId, ProtocolVersion.latest, NonNegativeInt.zero)
  private val daBeta =
    PhysicalSynchronizerId(daId, ProtocolVersion.parseUnchecked(3444).value, NonNegativeInt.zero)
  private val daDev = PhysicalSynchronizerId(daId, ProtocolVersion.dev, NonNegativeInt.zero)
  private val daName = SynchronizerAlias.tryCreate("da")

  private val acmeId =
    SynchronizerId(UniqueIdentifier.tryCreate("acme", DefaultTestIdentities.namespace))
  private val acmeStable =
    PhysicalSynchronizerId(acmeId, ProtocolVersion.latest, NonNegativeInt.zero)
  private val acmeName = SynchronizerAlias.tryCreate("acme")

  def synchronizerConnectionConfigStore(
      mk: => FutureUnlessShutdown[SynchronizerConnectionConfigStore]
  ): Unit = {

    "when merging SynchronizerConnectionConfig" should {
      def addEndpoint(cfg: SynchronizerConnectionConfig) = cfg.copy(
        sequencerConnections = cfg.sequencerConnections
          .modify(sequencerAlias1, _.addEndpoints("https://host3:700").value)
      )
      def withSequencerId(cfg: SynchronizerConnectionConfig) = cfg.copy(
        sequencerConnections = cfg.sequencerConnections
          .modify(sequencerAlias1, _.withSequencerId(DefaultTestIdentities.sequencerId))
      )

      "merge subsumed configs successfully" in {
        val configWithSequencerId = withSequencerId(config)
        val configWithEndpoint = addEndpoint(config)

        // test that the subsumed connection has sequencerId set
        config.subsumeMerge(configWithSequencerId) shouldBe Right(configWithSequencerId)
        // test that the subsuming connection has sequencerId set
        configWithSequencerId.subsumeMerge(config) shouldBe Right(configWithSequencerId)

        // test that additional endpoints in the subsuming config are retained
        configWithEndpoint.subsumeMerge(config) shouldBe Right(configWithEndpoint)
        // test that additional endpoints in the subsumed config leads to an error
        config.subsumeMerge(configWithEndpoint).left.value should not be empty

        val configWithTwoSequencers = config.copy(
          sequencerConnections = SequencerConnections.tryMany(
            Seq(
              connection1,
              connection1.copy(
                connection1.endpoints.map(e => e.copy(port = e.port + 10)),
                sequencerAlias = SequencerAlias.create("sequencer2").value,
              ),
            ),
            sequencerTrustThreshold = PositiveInt.one,
            sequencerLivenessMargin = NonNegativeInt.zero,
            submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
            sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
          )
        )

        // test that a config with multiple sequencers subsumes the config with just one sequencer with sequencer id
        configWithTwoSequencers.subsumeMerge(withSequencerId(config)) shouldBe Right(
          withSequencerId(configWithTwoSequencers)
        )
      }
    }

    "when storing connection configs" should {

      "be able to store and retrieve a config successfully" in {
        val synchronizerId2 = PhysicalSynchronizerId(
          psid.logical,
          psid.protocolVersion,
          psid.serial.increment.toNonNegative,
        )

        val predecessor = SynchronizerPredecessor(
          psid = psid,
          upgradeTime = CantonTimestamp.now(),
        )

        for {
          sut <- mk
          _ <- valueOrFail(
            sut.put(config, Active, KnownPhysicalSynchronizerId(synchronizerId2), Some(predecessor))
          )(
            "failed to add config to synchronizer config store"
          )

          _ = sut
            .get(daAlias, KnownPhysicalSynchronizerId(synchronizerId2))
            .value
            .config shouldBe config

          expectedResult = StoredSynchronizerConnectionConfig(
            config,
            Active,
            KnownPhysicalSynchronizerId(synchronizerId2),
            Some(predecessor),
          )

        } yield sut.get(synchronizerId2).value shouldBe expectedResult
      }

      "store the same config twice for idempotency" in {
        for {
          sut <- mk
          _ <- sut
            .put(config, Active, KnownPhysicalSynchronizerId(psid), None)
            .valueOrFail("first store of config")
          _ <- sut
            .put(config, Active, KnownPhysicalSynchronizerId(psid), None)
            .valueOrFail("second store of config")
        } yield succeed
      }

      "return error if config for (alias, id) already exists with a different value" in {
        for {
          sut <- mk
          _ <- sut
            .put(config, Active, KnownPhysicalSynchronizerId(psid), None)
            .valueOrFail("first store of config")
          result <- sut
            .put(
              config.copy(manualConnect = true),
              Active,
              KnownPhysicalSynchronizerId(psid),
              None,
            )
            .value
        } yield {
          result shouldBe Left(
            ConfigAlreadyExists(daAlias, KnownPhysicalSynchronizerId(psid))
          )
        }
      }

      "return an error if the predecessor has incompatible PSId" in {
        val predecessor = SynchronizerPredecessor(
          psid = daStable,
          upgradeTime = CantonTimestamp.now(),
        )

        for {
          sut <- mk

          error <- sut
            .put(config, Active, KnownPhysicalSynchronizerId(acmeStable), Some(predecessor))
            .value

        } yield error.left.value shouldBe InconsistentPredecessorLogicalSynchronizerIds(
          acmeStable,
          daStable,
        )
      }

      "allow to store physical synchronizer id when it is known" in {
        for {
          sut <- mk

          _ <- sut
            .put(getConfig(daStable), Active, UnknownPhysicalSynchronizerId, None)
            .valueOrFail("put") // no physical id known

          _ <- sut.setPhysicalSynchronizerId(daName, daStable).valueOrFail("set psid (1st)")
          // idempotency
          _ <- sut.setPhysicalSynchronizerId(daName, daStable).valueOrFail("set psid (2nd)")
          _ = sut
            .getActive(daName)
            .value
            .configuredPSId shouldBe KnownPhysicalSynchronizerId(daStable)

          unknownAlias <- sut.setPhysicalSynchronizerId(acmeName, acmeStable).value
          _ = unknownAlias.left.value shouldBe MissingConfigForSynchronizer(
            ConfigIdentifier.WithAlias(
              acmeName,
              UnknownPhysicalSynchronizerId,
            )
          )
        } yield succeed
      }

      "return an error when trying to store a PSId which is incompatible with the predecessor" in {
        val predecessor = SynchronizerPredecessor(acmeStable, CantonTimestamp.now())

        for {
          sut <- mk

          _ <- sut
            .put(getConfig(daStable), Active, UnknownPhysicalSynchronizerId, Some(predecessor))
            .valueOrFail("put") // no physical id known

          error <- sut.setPhysicalSynchronizerId(daName, daStable).value
        } yield error.left.value shouldBe InconsistentPredecessorLogicalSynchronizerIds(
          daStable,
          acmeStable,
        )
      }

      "return error when trying to assign the same physical id to different aliases" in {
        val c1 = config
        val c2 = config.copy(synchronizerAlias = acmeAlias)

        c1.synchronizerAlias should not be c2.synchronizerAlias

        for {
          sut <- mk
          _ <- sut
            .put(c1, Active, KnownPhysicalSynchronizerId(psid), None)
            .valueOrFail("first store of config")

          putError <- sut
            .put(c2, Active, KnownPhysicalSynchronizerId(psid), None)
            .value

          _ = putError.left.value shouldBe SynchronizerIdAlreadyAdded(psid, daAlias)

          _ <- sut
            .put(c2, Active, UnknownPhysicalSynchronizerId, None)
            .valueOrFail("second store of config")

          // PSId synchronizer id already used for c1.alias
          setIdError <- sut.setPhysicalSynchronizerId(c2.synchronizerAlias, psid).value

        } yield setIdError.left.value shouldBe SynchronizerIdAlreadyAdded(psid, daAlias)
      }

      "return error when trying to have multiple active configs for a synchronizer alias" in {
        val c1 = config
        val psid_1 = psid.copy(serial = NonNegativeInt.one)
        val c2 = config.copy(synchronizerId = Some(psid_1))

        c1.synchronizerId should not be c2.synchronizerId

        for {
          sut <- mk
          _ <- sut
            .put(c1, Active, KnownPhysicalSynchronizerId(psid), None)
            .valueOrFail("first store of config")

          putError <- sut
            .put(c2, Active, KnownPhysicalSynchronizerId(psid_1), None)
            .value

          _ = putError.left.value shouldBe AtMostOnePhysicalActive(
            daAlias,
            Set(
              KnownPhysicalSynchronizerId(psid),
              KnownPhysicalSynchronizerId(psid_1),
            ),
          )

          putError <- sut
            .put(c2, Active, UnknownPhysicalSynchronizerId, None)
            .value

          _ = putError.left.value shouldBe AtMostOnePhysicalActive(
            daAlias,
            Set(
              KnownPhysicalSynchronizerId(psid),
              UnknownPhysicalSynchronizerId,
            ),
          )
        } yield succeed
      }

      "return error if config being retrieved does not exist" in {
        for {
          sut <- mk
        } yield {
          sut.get(daAlias, UnknownPhysicalSynchronizerId) shouldBe Left(
            MissingConfigForSynchronizer(
              ConfigIdentifier.WithAlias(daAlias, UnknownPhysicalSynchronizerId)
            )
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
          sequencerAlias1,
          None,
        )
        val secondConfig = SynchronizerConnectionConfig(
          daAlias,
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
          _ <- valueOrFail(
            sut.put(config, Active, KnownPhysicalSynchronizerId(psid), None)
          )(
            "failed to add config to synchronizer config store"
          )
          _ <- valueOrFail(sut.replace(KnownPhysicalSynchronizerId(psid), secondConfig))(
            "failed to replace config in config store"
          )
          retrievedConfig <- FutureUnlessShutdown.pure(
            valueOrFail(sut.get(daAlias, KnownPhysicalSynchronizerId(psid)))(
              "failed to retrieve config from synchronizer config store"
            )
          )
        } yield retrievedConfig.config shouldBe secondConfig
      }

      "return error if replaced config does not exist" in {
        for {
          sut <- mk
          result <- sut.replace(KnownPhysicalSynchronizerId(psid), config).value
        } yield result shouldBe Left(
          MissingConfigForSynchronizer(
            ConfigIdentifier.WithAlias(daAlias, KnownPhysicalSynchronizerId(psid))
          )
        )
      }

      "be able to retrieve all configs" in {
        val secondConfig = config.copy(synchronizerAlias = SynchronizerAlias.tryCreate("another"))
        for {
          sut <- mk
          _ <- valueOrFail(
            sut.put(config, Active, KnownPhysicalSynchronizerId(psid), None)
          )(
            "failed to add config to synchronizer config store"
          )
          _ <- valueOrFail(
            sut.put(secondConfig, Active, KnownPhysicalSynchronizerId(psid2), None)
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
            .put(config, Active, KnownPhysicalSynchronizerId(psid), None)
            .valueOrFail("put")
          _ <- sut.refreshCache()
          fetchedConfig = valueOrFail(
            sut.get(config.synchronizerAlias, KnownPhysicalSynchronizerId(psid))
          )("get")
        } yield fetchedConfig.config shouldBe config
      }
    }

    "support of several physical synchronizers of the same logical instance" should {
      def getStoredSynchronizerConnectionConfig(psid: PhysicalSynchronizerId) =
        StoredSynchronizerConnectionConfig(
          getConfig(psid),
          Inactive,
          KnownPhysicalSynchronizerId(psid),
          None,
        )

      "work (simple case)" in {
        for {
          sut <- mk

          _ <- sut
            .put(getConfig(daStable), Inactive, KnownPhysicalSynchronizerId(daStable), None)
            .valueOrFail("put")
          _ <- sut
            .put(getConfig(daDev), Inactive, KnownPhysicalSynchronizerId(daDev), None)
            .valueOrFail("put")
          _ <- sut
            .put(getConfig(daBeta), Inactive, UnknownPhysicalSynchronizerId, None)
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
            Inactive,
            UnknownPhysicalSynchronizerId,
            None,
          )
        } yield succeed
      }

      "store the same config twice for idempotency" in {
        for {
          sut <- mk

          _ <- sut
            .put(getConfig(daStable, true), Active, KnownPhysicalSynchronizerId(daStable), None)
            .valueOrFail("put (first)")
          _ <- sut
            .put(getConfig(daStable, true), Active, KnownPhysicalSynchronizerId(daStable), None)
            .valueOrFail("put (second)")

          // different config, should be rejected
          resStable <- sut
            .put(getConfig(daStable, false), Active, KnownPhysicalSynchronizerId(daStable), None)
            .value
          _ = resStable shouldBe Left(
            ConfigAlreadyExists(daName, KnownPhysicalSynchronizerId(daStable))
          )

        } yield succeed
      }

      "return an error when trying to store inconsistent physical synchronizer ids for the same alias" in {
        for {
          sut <- mk

          _ <- sut
            .put(getConfig(daStable), Active, KnownPhysicalSynchronizerId(daStable), None)
            .valueOrFail("put (first)")

          error <- sut
            .put(getConfig(daStable), Inactive, KnownPhysicalSynchronizerId(acmeStable), None)
            .value

          _ = error.left.value shouldBe InconsistentLogicalSynchronizerIds(
            daName,
            newPSId = acmeStable,
            existingPSId = daStable,
          )

          _ <- sut
            .put(getConfig(daStable), Inactive, KnownPhysicalSynchronizerId(daDev), None)
            .valueOrFail("put (third)")

        } yield succeed
      }

      "allow to replace configs" in {
        for {
          sut <- mk

          _ <- sut
            .put(getConfig(daStable), Active, UnknownPhysicalSynchronizerId, None)
            .valueOrFail("put") // no physical id known
          _ <- sut
            .put(getConfig(daDev), Inactive, KnownPhysicalSynchronizerId(daDev), None)
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

      "allow to set sequencer ids" in {
        val s1Id = SequencerId(UniqueIdentifier.tryCreate("sequencer1", namespace))
        val s2Id = SequencerId(UniqueIdentifier.tryCreate("sequencer2", namespace))
        val s3Id = SequencerId(UniqueIdentifier.tryCreate("sequencer3", namespace))

        val sutF = mk
        val initialConfig = getConfig(daStable)

        def getSequencerId(
            alias: SequencerAlias
        ): EitherT[FutureUnlessShutdown, UnknownPSId, Option[SequencerId]] =
          for {
            sut <- EitherT
              .liftF[FutureUnlessShutdown, UnknownPSId, SynchronizerConnectionConfigStore](sutF)
            storedConfig <- EitherT.fromEither[FutureUnlessShutdown](sut.get(daStable))
          } yield storedConfig.config.sequencerConnections.aliasToConnection
            .get(alias)
            .value
            .sequencerId

        for {
          sut <- sutF

          failureUnset <- sut
            .setSequencerIds(
              daStable,
              Map(sequencerAlias1 -> s1Id),
            )
            .value

          _ = failureUnset.left.value shouldBe MissingConfigForSynchronizer(
            ConfigIdentifier.WithPSId(daStable)
          )

          _ <- sut
            .put(initialConfig, Inactive, KnownPhysicalSynchronizerId(daStable), None)
            .valueOrFail("put daStable")

          initialId1 <- getSequencerId(sequencerAlias1).valueOrFail("get initial sequencer id")

          _ = initialId1 shouldBe empty

          _ <- sut
            .setSequencerIds(
              daStable,
              Map(sequencerAlias1 -> s1Id, sequencerAlias2 -> s2Id),
            )
            .valueOrFail("set initial sequencer id")

          retrievedId1 <- getSequencerId(sequencerAlias1).valueOrFail("get sequencer id")
          retrievedId2 <- getSequencerId(sequencerAlias2).valueOrFail("get sequencer id")
          _ = retrievedId1.value shouldBe s1Id
          _ = retrievedId2.value shouldBe s2Id

          // idempotency
          _ <- sut
            .setSequencerIds(
              daStable,
              Map(sequencerAlias1 -> s1Id, sequencerAlias2 -> s2Id),
            )
            .valueOrFail("second call to set id")

          // failure for different ids
          failureInconsistent <- sut
            .setSequencerIds(
              daStable,
              Map(
                // different from s1Id
                sequencerAlias1 -> s3Id
              ),
            )
            .value

          _ = failureInconsistent.left.value shouldBe InconsistentSequencerIds(
            ConfigIdentifier.WithPSId(daStable),
            Map(sequencerAlias1 -> s3Id),
            "Mismatch",
          )

          retrievedIdFinal1 <- getSequencerId(sequencerAlias1).valueOrFail("get sequencer id")
          _ = retrievedIdFinal1.value shouldBe s1Id
        } yield succeed
      }

      "getActive return active synchronizer" in {
        for {
          sut <- mk

          unknown = sut.getActive(daName)
          _ = unknown.left.value shouldBe UnknownAlias(daName)

          _ <- sut
            .put(getConfig(daStable), Inactive, UnknownPhysicalSynchronizerId, None)
            .valueOrFail("put daStable")
          _ = sut
            .getActive(daName)
            .left
            .value shouldBe NoActiveSynchronizer(daName)

          _ <- sut
            .setStatus(daName, UnknownPhysicalSynchronizerId, Active)
            .valueOrFail("set active da Unknown")
          _ = sut
            .getActive(daName)
            .value shouldBe StoredSynchronizerConnectionConfig(
            getConfig(daStable),
            Active,
            UnknownPhysicalSynchronizerId,
            None,
          )

          _ <- sut
            .setStatus(daName, UnknownPhysicalSynchronizerId, Inactive)
            .valueOrFail("set inactive da Unknown")

          _ <- sut
            .put(getConfig(daDev), Active, KnownPhysicalSynchronizerId(daDev), None)
            .valueOrFail("put daDev")

          setStatusError <- sut
            .setStatus(daName, UnknownPhysicalSynchronizerId, Active)
            .swap
            .valueOrFail("set active da Unknown")

          _ = setStatusError shouldBe AtMostOnePhysicalActive(
            daName,
            Set(KnownPhysicalSynchronizerId(daDev), UnknownPhysicalSynchronizerId),
          )

          _ = sut
            .getActive(daName)
            .value shouldBe StoredSynchronizerConnectionConfig(
            getConfig(daDev),
            Active,
            KnownPhysicalSynchronizerId(daDev),
            None,
          )

        } yield succeed
      }
    }
  }
}
