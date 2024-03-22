// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.caching
import com.daml.caching.SizedCache._
import com.daml.lf.ledger.EventId
import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.metrics.Metrics

/** Definitions of caches used for serializing and deserializing Daml-LF values.
  * The cache is shared between the read and write path:
  * The indexer writes values into the cache, the ledger API server looks up values.
  */
object LfValueTranslationCache {

  final case class Cache(events: EventCache, contracts: ContractCache)
  type EventCache = caching.Cache[EventCache.Key, EventCache.Value]
  type ContractCache = caching.Cache[ContractCache.Key, ContractCache.Value]

  final case class Config(
      eventsMaximumSize: Long = Configuration.none.maximumSize,
      contractsMaximumSize: Long = Configuration.none.maximumSize,
  )

  object Cache {

    def none: Cache = Cache(caching.Cache.none, caching.Cache.none)

    def newInstance(
        config: Config
    ): Cache =
      Cache(
        events = EventCache.newInstance(Configuration(config.eventsMaximumSize)),
        contracts = ContractCache.newInstance(Configuration(config.contractsMaximumSize)),
      )

    def newInstrumentedInstance(
        config: Config,
        metrics: Metrics,
    ): Cache =
      Cache(
        events = EventCache.newInstrumentedInstance(
          Configuration(config.eventsMaximumSize),
          metrics,
        ),
        contracts = ContractCache.newInstrumentedInstance(
          Configuration(config.contractsMaximumSize),
          metrics,
        ),
      )
  }

  object EventCache {

    def newInstance(configuration: Configuration): EventCache =
      from(configuration)

    def newInstrumentedInstance(
        configuration: Configuration,
        metrics: Metrics,
    ): EventCache =
      from(
        configuration = configuration,
        metrics = metrics.daml.index.db.translation.cache,
      )

    final class UnexpectedTypeException(value: Value)
        extends RuntimeException(s"Unexpected value $value")

    final case class Key(eventId: EventId)

    sealed abstract class Value {
      def assertCreate(): Value.Create
      def assertExercise(): Value.Exercise
    }

    object Value {
      final case class Create(
          argument: VersionedValue,
          key: Option[VersionedValue],
      ) extends Value {
        override def assertCreate(): Create = this
        override def assertExercise(): Exercise = throw new UnexpectedTypeException(this)
      }
      final case class Exercise(
          argument: VersionedValue,
          result: Option[VersionedValue],
      ) extends Value {
        override def assertCreate(): Create = throw new UnexpectedTypeException(this)
        override def assertExercise(): Exercise = this
      }
    }

  }

  object ContractCache {

    def newInstance(configuration: Configuration): ContractCache =
      from(configuration)

    def newInstrumentedInstance(
        configuration: Configuration,
        metrics: Metrics,
    ): ContractCache =
      from(
        configuration = configuration,
        metrics = metrics.daml.index.db.translation.cache,
      )

    final case class Key(contractId: ContractId)

    final case class Value(argument: VersionedValue)
  }
}
