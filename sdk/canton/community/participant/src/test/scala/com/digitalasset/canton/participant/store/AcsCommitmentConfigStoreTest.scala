// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.pruning.{
  ConfigForNoWaitCounterParticipants,
  ConfigForSlowCounterParticipants,
  ConfigForSynchronizerThresholds,
}
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}

import scala.concurrent.ExecutionContext

trait SlowCounterParticipantConfigTest extends CommitmentStoreBaseTest {
  lazy val synchronizerId2: SynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer2::synchronizer2")
  )

  def AcsCommitmentSlowCounterParticipantConfigStore(
      mkWith: ExecutionContext => AcsCommitmentSlowCounterParticipantConfigStore
  ): Unit = {
    "AcsCommitmentSlowCounterParticipantConfigStore" should {

      def mk() = mkWith(executionContext)

      "be able to add a config" in {
        val store = mk()
        val config1 = ConfigForSlowCounterParticipants(
          synchronizerId,
          remoteId,
          isDistinguished = true,
          isAddedToMetrics = true,
        )
        val threshold1 = ConfigForSynchronizerThresholds(
          synchronizerId,
          NonNegativeLong.tryCreate(10),
          NonNegativeLong.tryCreate(10),
        )
        for {
          start <- store.fetchAllSlowCounterParticipantConfig()
          _ <- store.createOrUpdateCounterParticipantConfigs(Seq(config1), Seq(threshold1))
          end <- store.fetchAllSlowCounterParticipantConfig()
        } yield {
          start shouldBe (Seq.empty, Seq.empty)
          end shouldBe (Seq(config1), Seq(threshold1))
        }
      }.failOnShutdown("Aborted due to shutdown")

      "only add one config if synchronizerId is the same" in {
        val store = mk()
        val config1 = ConfigForSlowCounterParticipants(
          synchronizerId,
          remoteId,
          isDistinguished = true,
          isAddedToMetrics = true,
        )
        val config2 = ConfigForSlowCounterParticipants(
          synchronizerId,
          remoteId2,
          isDistinguished = true,
          isAddedToMetrics = true,
        )
        val threshold1 = ConfigForSynchronizerThresholds(
          synchronizerId,
          NonNegativeLong.tryCreate(10),
          NonNegativeLong.tryCreate(10),
        )
        val threshold2 = ConfigForSynchronizerThresholds(
          synchronizerId,
          NonNegativeLong.tryCreate(15),
          NonNegativeLong.tryCreate(15),
        )
        for {
          start <- store.fetchAllSlowCounterParticipantConfig()
          _ <- store.createOrUpdateCounterParticipantConfigs(Seq(config1), Seq(threshold1))
          _ <- store.createOrUpdateCounterParticipantConfigs(Seq(config2), Seq(threshold2))
          end <- store.fetchAllSlowCounterParticipantConfig()
        } yield {
          start shouldBe (Seq.empty, Seq.empty)
          end shouldBe (Seq(config2), Seq(threshold2))
        }

      }.failOnShutdown("Aborted due to shutdown.")

      "add two configs for the same synchronizer should only store latest" in {
        val store = mk()
        val config1 = ConfigForSlowCounterParticipants(
          synchronizerId,
          remoteId,
          isDistinguished = false,
          isAddedToMetrics = false,
        )
        val config2 = ConfigForSlowCounterParticipants(
          synchronizerId,
          remoteId,
          isDistinguished = true,
          isAddedToMetrics = true,
        )
        val threshold = ConfigForSynchronizerThresholds(
          synchronizerId,
          NonNegativeLong.tryCreate(10),
          NonNegativeLong.tryCreate(10),
        )
        for {
          start <- store.fetchAllSlowCounterParticipantConfig()
          _ <- store.createOrUpdateCounterParticipantConfigs(Seq(config1, config2), Seq(threshold))
          end <- store.fetchAllSlowCounterParticipantConfig()
        } yield {
          start shouldBe (Seq.empty, Seq.empty)
          end shouldBe (Seq(config2), Seq(threshold))
        }

      }.failOnShutdown("Aborted due to shutdown.")

      "be able to remove specified synchronizer" in {
        val store = mk()
        val config1 = ConfigForSlowCounterParticipants(
          synchronizerId,
          remoteId,
          isDistinguished = true,
          isAddedToMetrics = true,
        )
        val config2 = ConfigForSlowCounterParticipants(
          synchronizerId2,
          remoteId2,
          isDistinguished = true,
          isAddedToMetrics = true,
        )

        val threshold1 = ConfigForSynchronizerThresholds(
          synchronizerId,
          NonNegativeLong.tryCreate(10),
          NonNegativeLong.tryCreate(10),
        )
        val threshold2 = ConfigForSynchronizerThresholds(
          synchronizerId2,
          NonNegativeLong.tryCreate(15),
          NonNegativeLong.tryCreate(15),
        )
        for {
          _ <- store.createOrUpdateCounterParticipantConfigs(Seq(config1), Seq(threshold1))
          added1 <- store.fetchAllSlowCounterParticipantConfig()
          _ <- store.createOrUpdateCounterParticipantConfigs(Seq(config2), Seq(threshold2))
          added2 <- store.fetchAllSlowCounterParticipantConfig()
          _ <- store.clearSlowCounterParticipants(Seq(synchronizerId2))
          afterRemove <- store.fetchAllSlowCounterParticipantConfig()
        } yield {
          added1 shouldBe (Seq(config1), Seq(threshold1))
          added2 shouldBe (Seq(config1, config2), Seq(threshold1, threshold2))
          afterRemove shouldBe (Seq(config1), Seq(threshold1))
        }
      }.failOnShutdown("Aborted due to shutdown.")

      "remove all synchronizers if empty seq is applied" in {
        val store = mk()
        val config1 = ConfigForSlowCounterParticipants(
          synchronizerId,
          remoteId,
          isDistinguished = true,
          isAddedToMetrics = true,
        )
        val config2 = ConfigForSlowCounterParticipants(
          synchronizerId2,
          remoteId2,
          isDistinguished = true,
          isAddedToMetrics = true,
        )

        val threshold1 = ConfigForSynchronizerThresholds(
          synchronizerId,
          NonNegativeLong.tryCreate(10),
          NonNegativeLong.tryCreate(10),
        )
        val threshold2 = ConfigForSynchronizerThresholds(
          synchronizerId2,
          NonNegativeLong.tryCreate(15),
          NonNegativeLong.tryCreate(15),
        )
        for {
          _ <- store.createOrUpdateCounterParticipantConfigs(
            Seq(config1, config2),
            Seq(threshold1, threshold2),
          )
          added <- store.fetchAllSlowCounterParticipantConfig()
          _ <- store.clearSlowCounterParticipants(Seq.empty)
          afterRemove <- store.fetchAllSlowCounterParticipantConfig()
        } yield {
          added shouldBe (Seq(config1, config2), Seq(threshold1, threshold2))
          afterRemove shouldBe (Seq.empty, Seq.empty)
        }
      }.failOnShutdown("Aborted due to shutdown.")

    }
  }
  def AcsCommitmentNoWaitParticipantConfigStore(
      mkWith: ExecutionContext => AcsCommitmentNoWaitCounterParticipantConfigStore
  ): Unit = {
    "AcsCommitmentNoWaitParticipantConfigStore" should {

      def mk() = mkWith(executionContext)

      val config1 = ConfigForNoWaitCounterParticipants(
        synchronizerId,
        remoteId,
      )
      val config2 = ConfigForNoWaitCounterParticipants(
        synchronizerId2,
        remoteId,
      )

      "be able to add a config" in {
        val store = mk()
        for {
          start <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
          _ <- store.addNoWaitCounterParticipant(Seq(config1))
          end <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
        } yield {
          start shouldBe Seq.empty
          end.toSet shouldBe Set(config1)
        }
      }.failOnShutdown("Aborted due to shutdown.")

      "be able to add multi-config" in {
        val store = mk()
        for {
          start <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
          _ <- store.addNoWaitCounterParticipant(Seq(config2, config1))
          end <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
        } yield {
          start shouldBe Seq.empty
          end.toSet shouldBe Set(config1, config2)
        }
      }.failOnShutdown("Aborted due to shutdown.")

      "be able to reset" in {
        val store = mk()
        for {
          _ <- store.addNoWaitCounterParticipant(Seq(config1))
          start <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
          _ <- store.removeNoWaitCounterParticipant(Seq(synchronizerId), Seq(remoteId))
          end <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
        } yield {
          start.toSet shouldBe Set(config1)
          end shouldBe Seq.empty
        }
      }.failOnShutdown("Aborted due to shutdown.")

      "be able to filter active no waits by synchronizer" in {
        val store = mk()
        for {
          _ <- store.addNoWaitCounterParticipant(Seq(config1, config2))
          unfiltered <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
          filtered <- store.getAllActiveNoWaitCounterParticipants(Seq(synchronizerId), Seq.empty)
        } yield {
          unfiltered.toSet shouldBe Set(config2, config1)
          filtered.toSet shouldBe Set(config1)
        }
      }.failOnShutdown("Aborted due to shutdown.")

      "be able to filter active no waits by participant" in {
        val store = mk()
        val specialConfig = ConfigForNoWaitCounterParticipants(
          synchronizerId2,
          remoteId2,
        )

        for {
          _ <- store.addNoWaitCounterParticipant(Seq(config1, specialConfig))
          unfiltered <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
          filtered <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq(remoteId2))
        } yield {
          unfiltered.toSet shouldBe Set(specialConfig, config1)
          filtered.toSet shouldBe Set(specialConfig)
        }
      }.failOnShutdown("Aborted due to shutdown.")

      "be able to filter active no waits by participant and synchronizer" in {
        val store = mk()
        val specialConfig = ConfigForNoWaitCounterParticipants(
          synchronizerId2,
          remoteId,
        )
        val specialConfig2 = ConfigForNoWaitCounterParticipants(
          synchronizerId2,
          remoteId2,
        )

        for {
          _ <- store.addNoWaitCounterParticipant(
            Seq(config1, config2, specialConfig, specialConfig2)
          )
          unfiltered <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
          filtered <- store.getAllActiveNoWaitCounterParticipants(
            Seq(synchronizerId2),
            Seq(remoteId2),
          )
        } yield {
          unfiltered.toSet shouldBe Set(config1, config2, specialConfig, specialConfig2)
          filtered.toSet shouldBe Set(specialConfig2)
        }
      }.failOnShutdown("Aborted due to shutdown.")

      "empty synchronizer list should not reset" in {
        val store = mk()
        for {
          _ <- store.addNoWaitCounterParticipant(Seq(config1))
          _ <- store.addNoWaitCounterParticipant(Seq(config2))
          start <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
          _ <- store.removeNoWaitCounterParticipant(Seq.empty, Seq(remoteId))
          end <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
        } yield {
          start.toSet shouldBe Set(config1, config2)
          end.toSet shouldBe Set(config1, config2)
        }
      }.failOnShutdown("Aborted due to shutdown.")

      "empty participant list should not reset" in {
        val store = mk()
        for {
          _ <- store.addNoWaitCounterParticipant(Seq(config1))
          _ <- store.addNoWaitCounterParticipant(Seq(config2))
          start <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
          _ <- store.removeNoWaitCounterParticipant(Seq(synchronizerId), Seq.empty)
          end <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
        } yield {
          start.toSet shouldBe Set(config1, config2)
          end.toSet shouldBe Set(config1, config2)
        }
      }.failOnShutdown("Aborted due to shutdown.")

      "overwrite in case of matching synchronizer and participant" in {
        val store = mk()
        val config = ConfigForNoWaitCounterParticipants(
          synchronizerId,
          remoteId,
        )

        for {
          _ <- store.addNoWaitCounterParticipant(Seq(config1))
          start <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
          _ <- store.addNoWaitCounterParticipant(Seq(config))
          end <- store.getAllActiveNoWaitCounterParticipants(Seq.empty, Seq.empty)
        } yield {
          start.toSet shouldBe Set(config1)
          end.toSet shouldBe Set(config)
        }
      }.failOnShutdown("Aborted due to shutdown.")

    }

  }
}
