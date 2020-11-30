// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.caching
import com.daml.ledger.EventId
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.value.{Record => ApiRecord, Value => ApiValue}
import com.daml.metrics.Metrics
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.dao.events.{Value => LfValue}
import com.daml.platform.store.serialization.ValueSerializer

final class LfValueTranslation(val cache: LfValueTranslation.Cache) {

  private def cantSerialize(attribute: String, forContract: ContractId): String =
    s"Cannot serialize $attribute for ${forContract.coid}"

  private def serializeCreateArgOrThrow(contractId: ContractId, arg: LfValue): Array[Byte] =
    ValueSerializer.serializeValue(
      value = arg,
      errorContext = cantSerialize(attribute = "create argument", forContract = contractId),
    )

  private def serializeCreateArgOrThrow(c: Create): Array[Byte] =
    serializeCreateArgOrThrow(c.coid, c.coinst.arg)

  private def serializeNullableKeyOrThrow(c: Create): Option[Array[Byte]] =
    c.key.map(
      k =>
        ValueSerializer.serializeValue(
          value = k.key,
          errorContext = cantSerialize(attribute = "key", forContract = c.coid),
      )
    )

  private def serializeExerciseArgOrThrow(e: Exercise): Array[Byte] =
    ValueSerializer.serializeValue(
      value = e.chosenValue,
      errorContext = cantSerialize(attribute = "exercise argument", forContract = e.targetCoid),
    )

  private def serializeNullableExerciseResultOrThrow(e: Exercise): Option[Array[Byte]] =
    e.exerciseResult.map(
      exerciseResult =>
        ValueSerializer.serializeValue(
          value = exerciseResult,
          errorContext = cantSerialize(attribute = "exercise result", forContract = e.targetCoid),
      )
    )

  def serialize(contractId: ContractId, contractArgument: Value): Array[Byte] = {
    cache.contracts.put(
      key = LfValueTranslation.ContractCache.Key(contractId),
      value = LfValueTranslation.ContractCache.Value(contractArgument),
    )
    serializeCreateArgOrThrow(contractId, contractArgument)
  }

  def serialize(eventId: EventId, create: Create): (Array[Byte], Option[Array[Byte]]) = {
    cache.events.put(
      key = LfValueTranslation.EventCache.Key(eventId),
      value = LfValueTranslation.EventCache.Value.Create(create.coinst.arg, create.key.map(_.key)),
    )
    cache.contracts.put(
      key = LfValueTranslation.ContractCache.Key(create.coid),
      value = LfValueTranslation.ContractCache.Value(create.coinst.arg),
    )
    (serializeCreateArgOrThrow(create), serializeNullableKeyOrThrow(create))
  }

  def serialize(eventId: EventId, exercise: Exercise): (Array[Byte], Option[Array[Byte]]) = {
    cache.events.put(
      key = LfValueTranslation.EventCache.Key(eventId),
      value =
        LfValueTranslation.EventCache.Value.Exercise(exercise.chosenValue, exercise.exerciseResult),
    )
    (serializeExerciseArgOrThrow(exercise), serializeNullableExerciseResultOrThrow(exercise))
  }

  private def toApiValue(
      value: LfValue,
      verbose: Boolean,
      attribute: => String,
  ): ApiValue =
    LfEngineToApi.assertOrRuntimeEx(
      failureContext = s"attempting to deserialize persisted $attribute to value",
      LfEngineToApi
        .lfVersionedValueToApiValue(
          verbose = verbose,
          value = value,
        ),
    )

  private def toApiRecord(
      value: LfValue,
      verbose: Boolean,
      attribute: => String,
  ): ApiRecord =
    LfEngineToApi.assertOrRuntimeEx(
      failureContext = s"attempting to deserialize persisted $attribute to record",
      LfEngineToApi
        .lfVersionedValueToApiRecord(
          verbose = verbose,
          recordValue = value,
        ),
    )

  private def eventKey(s: String) = LfValueTranslation.EventCache.Key(EventId.assertFromString(s))

  def deserialize[E](raw: Raw.Created[E], verbose: Boolean): CreatedEvent = {
    val create =
      cache.events
        .getIfPresent(eventKey(raw.partial.eventId))
        .getOrElse(
          LfValueTranslation.EventCache.Value.Create(
            argument = ValueSerializer.deserializeValue(raw.createArgument),
            key = raw.createKeyValue.map(ValueSerializer.deserializeValue)
          )
        )
        .assertCreate()
    raw.partial.copy(
      createArguments = Some(
        toApiRecord(
          value = create.argument,
          verbose = verbose,
          attribute = "create argument",
        )
      ),
      contractKey = create.key.map(
        key =>
          toApiValue(
            value = key,
            verbose = verbose,
            attribute = "create key",
        )
      ),
    )
  }

  def deserialize(raw: Raw.TreeEvent.Exercised, verbose: Boolean): ExercisedEvent = {
    val exercise =
      cache.events
        .getIfPresent(eventKey(raw.partial.eventId))
        .getOrElse(
          LfValueTranslation.EventCache.Value.Exercise(
            argument = ValueSerializer.deserializeValue(raw.exerciseArgument),
            result = raw.exerciseResult.map(ValueSerializer.deserializeValue)
          )
        )
        .assertExercise()
    raw.partial.copy(
      choiceArgument = Some(
        toApiValue(
          value = exercise.argument,
          verbose = verbose,
          attribute = "exercise argument",
        )
      ),
      exerciseResult = exercise.result.map(
        result =>
          toApiValue(
            value = result,
            verbose = verbose,
            attribute = "exercise result",
        )
      ),
    )
  }

}

object LfValueTranslation {

  final case class Cache(events: EventCache, contracts: ContractCache)
  type EventCache = caching.Cache[EventCache.Key, EventCache.Value]
  type ContractCache = caching.Cache[ContractCache.Key, ContractCache.Value]

  object Cache {

    def none: Cache = Cache(caching.Cache.none, caching.Cache.none)

    def newInstance(
        eventConfiguration: caching.SizedCache.Configuration,
        contractConfiguration: caching.SizedCache.Configuration
    ): Cache =
      Cache(
        events = EventCache.newInstance(eventConfiguration),
        contracts = ContractCache.newInstance(contractConfiguration),
      )

    def newInstrumentedInstance(
        eventConfiguration: caching.SizedCache.Configuration,
        contractConfiguration: caching.SizedCache.Configuration,
        metrics: Metrics
    ): Cache =
      Cache(
        events = EventCache.newInstrumentedInstance(eventConfiguration, metrics),
        contracts = ContractCache.newInstrumentedInstance(contractConfiguration, metrics),
      )
  }

  object EventCache {

    def newInstance(configuration: caching.SizedCache.Configuration): EventCache =
      caching.SizedCache.from(configuration)

    def newInstrumentedInstance(
        configuration: caching.SizedCache.Configuration,
        metrics: Metrics
    ): EventCache =
      caching.SizedCache.from(
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
      final case class Create(argument: LfValue, key: Option[LfValue]) extends Value {
        override def assertCreate(): Create = this
        override def assertExercise(): Exercise = throw new UnexpectedTypeException(this)
      }
      final case class Exercise(argument: LfValue, result: Option[LfValue]) extends Value {
        override def assertCreate(): Create = throw new UnexpectedTypeException(this)
        override def assertExercise(): Exercise = this
      }
    }

  }

  object ContractCache {

    def newInstance(configuration: caching.SizedCache.Configuration): ContractCache =
      caching.SizedCache.from(configuration)

    def newInstrumentedInstance(
        configuration: caching.SizedCache.Configuration,
        metrics: Metrics
    ): ContractCache =
      caching.SizedCache.from(
        configuration = configuration,
        metrics = metrics.daml.index.db.translation.cache,
      )

    final case class Key(contractId: ContractId)

    final case class Value(argument: LfValue)
  }
}
