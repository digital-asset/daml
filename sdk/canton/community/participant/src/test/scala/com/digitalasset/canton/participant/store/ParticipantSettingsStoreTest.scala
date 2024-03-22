// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.option.*
import cats.syntax.parallel.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveNumeric}
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.store.ParticipantSettingsStore.Settings
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import monocle.Lens
import monocle.macros.GenLens

import scala.concurrent.Future

trait ParticipantSettingsStoreTest
    extends BaseTestWordSpec
    with HasExecutionContext // because we want to test concurrent insertions
    {

  lazy val resourceLimits0: ResourceLimits = ResourceLimits.default
  lazy val resourceLimits1: ResourceLimits =
    ResourceLimits(Some(NonNegativeInt.tryCreate(42)), None, PositiveNumeric.tryCreate(0.1))
  lazy val resourceLimits2: ResourceLimits =
    ResourceLimits(Some(NonNegativeInt.tryCreate(84)), None, PositiveNumeric.tryCreate(0.2))
  lazy val resourceLimits3: ResourceLimits =
    ResourceLimits(
      Some(NonNegativeInt.tryCreate(42)),
      Some(NonNegativeInt.tryCreate(22)),
      PositiveNumeric.tryCreate(0.3),
    )
  lazy val resourceLimits4: ResourceLimits =
    ResourceLimits(None, Some(NonNegativeInt.tryCreate(22)), PositiveNumeric.tryCreate(0.4))

  lazy val maxDedupDuration = NonNegativeFiniteDuration.tryOfMicros(123456789L)

  def participantSettingsStore(mk: () => ParticipantSettingsStore): Unit = {
    "resource limits" should {
      "support insert / delete / update" in {
        val store = mk()
        (for {
          _ <- store.refreshCache()
          settings0 = store.settings

          _ <- store.writeResourceLimits(resourceLimits1)
          settings1 = store.settings

          _ <- store.writeResourceLimits(resourceLimits2)
          settings2 = store.settings

          _ <- store.writeResourceLimits(resourceLimits3)
          settings3 = store.settings

          _ <- store.writeResourceLimits(resourceLimits4)
          settings4 = store.settings
        } yield {
          settings0 shouldBe Settings(resourceLimits = resourceLimits0)
          settings1 shouldBe Settings(resourceLimits = resourceLimits1)
          settings2 shouldBe Settings(resourceLimits = resourceLimits2)
          settings3 shouldBe Settings(resourceLimits = resourceLimits3)
          settings4 shouldBe Settings(resourceLimits = resourceLimits4)
        }).failOnShutdown.futureValue
      }
    }

    def singleInsertion[A](first: A, other: A)(
        insert: (ParticipantSettingsStore, A) => Future[Unit],
        lens: Lens[Settings, Option[A]],
    ): Unit = {

      "support a single insertion" in {
        val store = mk()
        (for {
          _ <- store.refreshCache().failOnShutdown
          settings0 = store.settings

          _ <- insert(store, first)
          settings1 = store.settings

          _ <- insert(store, first)
          settings2 = store.settings

          _ <- insert(store, other)
          settings3 = store.settings
        } yield {
          val emptyS = Settings()
          val firstS = lens.replace(first.some).apply(emptyS)
          settings0 shouldBe emptyS
          settings1 shouldBe firstS
          settings2 shouldBe firstS
          settings3 shouldBe firstS
        }).futureValue
      }

      "not affect resource limits" in {
        val store = mk()
        (for {
          _ <- store.writeResourceLimits(resourceLimits = resourceLimits1).failOnShutdown
          _ <- insert(store, first)
          settings1 = store.settings
          _ <- store.writeResourceLimits(resourceLimits4).failOnShutdown
          settings2 = store.settings
        } yield {
          val setter = lens.replace(first.some)
          settings1 shouldBe setter.apply(Settings(resourceLimits = resourceLimits1))
          settings2 shouldBe setter.apply(Settings(resourceLimits = resourceLimits4))
        }).futureValue
      }
    }

    "max deduplication duration" should {
      behave like singleInsertion(maxDedupDuration, NonNegativeFiniteDuration.Zero)(
        _.insertMaxDeduplicationDuration(_).failOnShutdown,
        GenLens[Settings](_.maxDeduplicationDuration),
      )
    }

    "unique contract keys" should {
      behave like singleInsertion(false, true)(
        _.insertUniqueContractKeysMode(_).failOnShutdown,
        GenLens[Settings](_.uniqueContractKeys),
      )
    }

    "eventually reach a consistent cache after concurrent updates" in {
      val store = mk()
      store.refreshCache().failOnShutdown.futureValue

      (1 until 10)
        .map(NonNegativeInt.tryCreate)
        .toList
        .parTraverse_(i => store.writeResourceLimits(ResourceLimits(Some(i), None)))
        .failOnShutdown
        .futureValue

      val cachedValue = store.settings

      store.refreshCache().failOnShutdown.futureValue
      store.settings shouldBe cachedValue
    }

    "fail if a value is queried before refreshing the cache" in {
      val store = mk()
      an[IllegalStateException] should be thrownBy { store.settings }
    }
  }
}
