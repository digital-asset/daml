// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.participant.store.SynchronizerAliasAndIdStore.InconsistentLogicalSynchronizerIds
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, SynchronizerAlias}
import org.scalatest.wordspec.AsyncWordSpec

trait RegisteredSynchronizersStoreTest extends FailOnShutdown {
  this: AsyncWordSpec & BaseTest =>

  protected implicit def traceContext: TraceContext

  private def alias(a: String): SynchronizerAlias = SynchronizerAlias.tryCreate(a)
  private def id(a: String, serial: Int = 0): PhysicalSynchronizerId = PhysicalSynchronizerId(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive(s"$a::default")),
    NonNegativeInt.tryCreate(serial),
    testedProtocolVersion,
  )

  def registeredSynchronizersStore(mk: () => RegisteredSynchronizersStore): Unit = {
    "be able to retrieve a map from alias to synchronizer ids" in {
      val sut = mk()
      for {
        _ <- valueOrFail(sut.addMapping(alias("first"), id("first")))("first")
        _ <- valueOrFail(sut.addMapping(alias("second"), id("second")))("second")
        map <- sut.aliasToSynchronizerIdMap
        expectedResult = Map(
          alias("first") -> NonEmpty.mk(Set, id("first")),
          alias("second") -> NonEmpty.mk(Set, id("second")),
        )
      } yield map shouldBe expectedResult
    }

    "be idempotent" in {
      val sut = mk()
      for {
        _ <- valueOrFail(sut.addMapping(alias("alias"), id("foo")))("foo 1")
        _ <- valueOrFail(sut.addMapping(alias("alias"), id("foo")))("foo 2")
      } yield succeed
    }

    "allow to register several physical synchronizers for the same logical synchronizer" in {
      val sut = mk()

      val psid0 = id("foo", serial = 0)
      val psid1 = id("foo", serial = 1)
      val incompatiblePSId = id("incompatible")

      for {
        _ <- valueOrFail(sut.addMapping(alias("alias"), psid0))("foo 0")
        _ <- valueOrFail(sut.addMapping(alias("alias"), psid1))("foo 1")
        queried1 <- sut.aliasToSynchronizerIdMap

        expectedResult = Map(
          alias("alias") -> NonEmpty.mk(Set, psid0, psid1)
        )
        _ = queried1 shouldBe expectedResult

        res <- sut.addMapping(alias("alias"), incompatiblePSId).value
        _ = res.left.value shouldBe InconsistentLogicalSynchronizerIds(
          alias("alias"),
          incompatiblePSId,
          psid0,
        )

        queried2 <- sut.aliasToSynchronizerIdMap
        _ = queried2 shouldBe expectedResult

      } yield succeed
    }

    "error if trying to add the same alias with a different logical synchronizer" in {
      val sut = mk()
      for {
        _ <- valueOrFail(sut.addMapping(alias("alias"), id("foo")))("foo")
        result <- sut.addMapping(alias("alias"), id("bar")).value
      } yield result shouldBe Left(
        InconsistentLogicalSynchronizerIds(
          alias("alias"),
          newPSId = id("bar"),
          existingPSId = id("foo"),
        )
      )
    }

    "error if trying to add the same synchronizer id again for a different alias" in {
      val sut = mk()
      for {
        _ <- valueOrFail(sut.addMapping(alias("foo"), id("id")))("foo -> id")
        result <- sut.addMapping(alias("bar"), id("id")).value
      } yield result shouldBe Left(
        SynchronizerAliasAndIdStore.SynchronizerIdAlreadyAdded(id("id"), alias("foo"))
      )
    }

  }
}
