// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.sandbox.stores.ledger.Ledger
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlStartMode
import com.digitalasset.platform.sandbox.stores.{InMemoryActiveLedgerState, InMemoryPackageStore}
import com.digitalasset.resources
import com.digitalasset.testing.postgresql.{PostgresFixture, PostgresResource}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

object LedgerResource {
  def inMemory(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState = InMemoryActiveLedgerState.empty,
      packages: InMemoryPackageStore = InMemoryPackageStore.empty,
      entries: ImmArray[LedgerEntryOrBump] = ImmArray.empty,
  )(implicit executionContext: ExecutionContext): Resource[Ledger] =
    fromResourceOwner(
      Ledger.inMemory(ledgerId, participantId, timeProvider, acs, packages, entries))

  def postgres(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      metrics: MetricRegistry,
      packages: InMemoryPackageStore = InMemoryPackageStore.empty,
  )(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
      logCtx: LoggingContext): Resource[Ledger] = {
    new Resource[Ledger] {
      @volatile
      private var postgres: Resource[PostgresFixture] = _

      @volatile
      private var ledger: Resource[Ledger] = _

      override def value: Ledger = ledger.value

      override def setup(): Unit = {
        postgres = PostgresResource()
        postgres.setup()

        ledger = fromResourceOwner(
          Ledger.jdbcBacked(
            postgres.value.jdbcUrl,
            ledgerId,
            participantId,
            timeProvider,
            InMemoryActiveLedgerState.empty,
            packages,
            ImmArray.empty,
            128,
            SqlStartMode.AlwaysReset,
            metrics
          ))
        ledger.setup()
      }

      override def close(): Unit = {
        ledger.close()
        postgres.close()
        postgres = null
        ledger = null
      }
    }
  }

  def fromResourceOwner[T](owner: resources.ResourceOwner[T])(
      implicit executionContext: ExecutionContext
  ): Resource[T] =
    new Resource[T] {
      @volatile
      var resource: resources.Resource[T] = _

      @volatile
      var _value: T = _

      override def value: T = _value

      override def setup(): Unit = {
        resource = owner.acquire()
        _value = Await.result(resource.asFuture, 10.seconds)
      }

      override def close(): Unit = {
        Await.result(resource.release(), 10.seconds)
      }
    }
}
