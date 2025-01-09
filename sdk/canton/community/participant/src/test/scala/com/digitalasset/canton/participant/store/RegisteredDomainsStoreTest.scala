// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.InUS
import com.digitalasset.canton.{BaseTest, SynchronizerAlias}
import org.scalatest.wordspec.AsyncWordSpec

trait RegisteredDomainsStoreTest extends InUS {
  this: AsyncWordSpec & BaseTest =>

  protected implicit def traceContext: TraceContext

  private def alias(a: String) = SynchronizerAlias.tryCreate(a)
  private def id(a: String) = SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive(s"$a::default"))

  def registeredDomainsStore(mk: () => RegisteredDomainsStore): Unit = {
    "be able to retrieve a map from alias to synchronizer ids" inUS {
      val sut = mk()
      for {
        _ <- valueOrFail(sut.addMapping(alias("first"), id("first")))("first")
        _ <- valueOrFail(sut.addMapping(alias("second"), id("second")))("second")
        map <- sut.aliasToSynchronizerIdMap
      } yield map should contain.only(
        alias("first") -> id("first"),
        alias("second") -> id("second"),
      )
    }

    "be idempotent" inUS {
      val sut = mk()
      for {
        _ <- valueOrFail(sut.addMapping(alias("alias"), id("foo")))("foo 1")
        _ <- valueOrFail(sut.addMapping(alias("alias"), id("foo")))("foo 2")
      } yield succeed
    }

    "error if trying to add the same alias with a different domain" inUS {
      val sut = mk()
      for {
        _ <- valueOrFail(sut.addMapping(alias("alias"), id("foo")))("foo")
        result <- sut.addMapping(alias("alias"), id("bar")).value
      } yield result shouldBe Left(
        SynchronizerAliasAndIdStore.SynchronizerAliasAlreadyAdded(alias("alias"), id("foo"))
      )
    }

    "error if trying to add the same synchronizer id again for a different alias" inUS {
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
