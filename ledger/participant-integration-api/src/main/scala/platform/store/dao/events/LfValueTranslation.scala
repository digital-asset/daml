// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream

import com.daml.caching
import com.daml.ledger.EventId
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.value.{
  Identifier => ApiIdentifier,
  Record => ApiRecord,
  Value => ApiValue,
}
import com.daml.lf.engine.ValueEnricher
import com.daml.lf.{engine => LfEngine}
import com.daml.lf.value.Value.VersionedValue
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.packages.DeduplicatingPackageLoader
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.dao.events.{
  ChoiceName => LfChoiceName,
  DottedName => LfDottedName,
  Identifier => LfIdentifier,
  ModuleName => LfModuleName,
  PackageId => LfPackageId,
  QualifiedName => LfQualifiedName,
  Value => LfValue,
}
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

import scala.concurrent.{ExecutionContext, Future}

final class LfValueTranslation(
    val cache: LfValueTranslation.Cache,
    metrics: Metrics,
    enricherO: Option[LfEngine.ValueEnricher],
    loadPackage: (
        LfPackageId,
        LoggingContext,
    ) => Future[Option[com.daml.daml_lf_dev.DamlLf.Archive]],
) {

  private[this] val packageLoader = new DeduplicatingPackageLoader()

  private def cantSerialize(attribute: String, forContract: ContractId): String =
    s"Cannot serialize $attribute for ${forContract.coid}"

  private def serializeCreateArgOrThrow(
      contractId: ContractId,
      arg: VersionedValue[ContractId],
  ): Array[Byte] =
    ValueSerializer.serializeValue(
      value = arg,
      errorContext = cantSerialize(attribute = "create argument", forContract = contractId),
    )

  private def serializeCreateArgOrThrow(c: Create): Array[Byte] =
    serializeCreateArgOrThrow(c.coid, c.versionedCoinst.arg)

  private def serializeNullableKeyOrThrow(c: Create): Option[Array[Byte]] =
    c.versionedKey.map(k =>
      ValueSerializer.serializeValue(
        value = k.key,
        errorContext = cantSerialize(attribute = "key", forContract = c.coid),
      )
    )

  private def serializeExerciseArgOrThrow(e: Exercise): Array[Byte] =
    ValueSerializer.serializeValue(
      value = e.versionedChosenValue,
      errorContext = cantSerialize(attribute = "exercise argument", forContract = e.targetCoid),
    )

  private def serializeNullableExerciseResultOrThrow(e: Exercise): Option[Array[Byte]] =
    e.versionedExerciseResult.map(exerciseResult =>
      ValueSerializer.serializeValue(
        value = exerciseResult,
        errorContext = cantSerialize(attribute = "exercise result", forContract = e.targetCoid),
      )
    )

  def serialize(
      contractId: ContractId,
      contractArgument: VersionedValue[ContractId],
  ): Array[Byte] = {
    cache.contracts.put(
      key = LfValueTranslation.ContractCache.Key(contractId),
      value = LfValueTranslation.ContractCache.Value(contractArgument),
    )
    serializeCreateArgOrThrow(contractId, contractArgument)
  }

  def serialize(eventId: EventId, create: Create): (Array[Byte], Option[Array[Byte]]) = {
    cache.events.put(
      key = LfValueTranslation.EventCache.Key(eventId),
      value = LfValueTranslation.EventCache.Value
        .Create(create.versionedCoinst.arg, create.versionedKey.map(_.key)),
    )
    cache.contracts.put(
      key = LfValueTranslation.ContractCache.Key(create.coid),
      value = LfValueTranslation.ContractCache.Value(create.versionedCoinst.arg),
    )
    (serializeCreateArgOrThrow(create), serializeNullableKeyOrThrow(create))
  }

  def serialize(eventId: EventId, exercise: Exercise): (Array[Byte], Option[Array[Byte]]) = {
    cache.events.put(
      key = LfValueTranslation.EventCache.Key(eventId),
      value = LfValueTranslation.EventCache.Value
        .Exercise(exercise.versionedChosenValue, exercise.versionedExerciseResult),
    )
    (serializeExerciseArgOrThrow(exercise), serializeNullableExerciseResultOrThrow(exercise))
  }

  private[this] def consumeEnricherResult[V](
      result: LfEngine.Result[V]
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[V] = {
    result match {
      case LfEngine.ResultDone(r) => Future.successful(r)
      case LfEngine.ResultError(e) => Future.failed(new RuntimeException(e.msg))
      case LfEngine.ResultNeedPackage(packageId, resume) =>
        packageLoader
          .loadPackage(
            packageId = packageId,
            delegate = packageId => loadPackage(packageId, loggingContext),
            metric = metrics.daml.index.db.translation.getLfPackage,
          )
          .flatMap(pkgO => consumeEnricherResult(resume(pkgO)))
      case result =>
        Future.failed(new RuntimeException(s"Unexpected ValueEnricher result: $result"))
    }
  }

  private[this] def toApiValue(
      value: LfValue,
      verbose: Boolean,
      attribute: => String,
      enrich: LfValue => LfEngine.Result[com.daml.lf.value.Value[ContractId]],
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[ApiValue] = for {
    enrichedValue <-
      if (verbose)
        consumeEnricherResult(enrich(value))
      else
        Future.successful(value.value)
  } yield {
    LfEngineToApi.assertOrRuntimeEx(
      failureContext = s"attempting to deserialize persisted $attribute to value",
      LfEngineToApi
        .lfValueToApiValue(
          verbose = verbose,
          value0 = enrichedValue,
        ),
    )
  }

  private[this] def toApiRecord(
      value: LfValue,
      verbose: Boolean,
      attribute: => String,
      enrich: LfValue => LfEngine.Result[com.daml.lf.value.Value[ContractId]],
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[ApiRecord] = for {
    enrichedValue <-
      if (verbose)
        consumeEnricherResult(enrich(value))
      else
        Future.successful(value.value)
  } yield {
    LfEngineToApi.assertOrRuntimeEx(
      failureContext = s"attempting to deserialize persisted $attribute to record",
      LfEngineToApi
        .lfValueToApiRecord(
          verbose = verbose,
          recordValue = enrichedValue,
        ),
    )
  }

  private[this] def apiIdentifierToDamlLfIdentifier(id: ApiIdentifier): LfIdentifier =
    LfIdentifier(
      LfPackageId.assertFromString(id.packageId),
      LfQualifiedName(
        LfModuleName.assertFromString(id.moduleName),
        LfDottedName.assertFromString(id.entityName),
      ),
    )

  private def eventKey(s: String) = LfValueTranslation.EventCache.Key(EventId.assertFromString(s))

  private def decompressAndDeserialize(algorithm: Compression.Algorithm, value: InputStream) =
    ValueSerializer.deserializeValue(algorithm.decompress(value))

  private[this] def enricher: ValueEnricher = {
    // Note: LfValueTranslation is used by JdbcLedgerDao for both serialization and deserialization.
    // Sometimes the JdbcLedgerDao is used in a way that it never needs to deserialize data in verbose mode
    // (e.g., the indexer, or some tests). In this case, the enricher is not required.
    enricherO.getOrElse(
      sys.error(
        "LfValueTranslation used to deserialize values in verbose mode without a ValueEnricher"
      )
    )
  }

  def deserialize[E](
      raw: Raw.Created[E],
      verbose: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[CreatedEvent] = {
    // Load the deserialized contract argument and contract key from the cache
    // This returns the values in DAML-LF format.
    val create =
      cache.events
        .getIfPresent(eventKey(raw.partial.eventId))
        .getOrElse(
          LfValueTranslation.EventCache.Value.Create(
            argument = decompressAndDeserialize(raw.createArgumentCompression, raw.createArgument),
            key = raw.createKeyValue.map(decompressAndDeserialize(raw.createKeyValueCompression, _)),
          )
        )
        .assertCreate()

    lazy val templateId: LfIdentifier = apiIdentifierToDamlLfIdentifier(raw.partial.templateId.get)

    // Convert DAML-LF values to ledger API values.
    // In verbose mode, this involves loading DAML-LF packages and filling in missing type information.
    for {
      createArguments <- toApiRecord(
        value = create.argument,
        verbose = verbose,
        attribute = "create argument",
        enrich = value => enricher.enrichContract(templateId, value.value),
      )
      contractKey <- create.key match {
        case Some(key) =>
          toApiValue(
            value = key,
            verbose = verbose,
            attribute = "create key",
            enrich = value => enricher.enrichContractKey(templateId, value.value),
          ).map(Some(_))
        case None => Future.successful(None)
      }
    } yield {
      raw.partial.copy(
        createArguments = Some(createArguments),
        contractKey = contractKey,
      )
    }
  }

  def deserialize(
      raw: Raw.TreeEvent.Exercised,
      verbose: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[ExercisedEvent] = {
    // Load the deserialized choice argument and choice result from the cache
    // This returns the values in DAML-LF format.
    val exercise =
      cache.events
        .getIfPresent(eventKey(raw.partial.eventId))
        .getOrElse(
          LfValueTranslation.EventCache.Value.Exercise(
            argument =
              decompressAndDeserialize(raw.exerciseArgumentCompression, raw.exerciseArgument),
            result =
              raw.exerciseResult.map(decompressAndDeserialize(raw.exerciseResultCompression, _)),
          )
        )
        .assertExercise()

    lazy val templateId: LfIdentifier = apiIdentifierToDamlLfIdentifier(raw.partial.templateId.get)
    lazy val choiceName: LfChoiceName = LfChoiceName.assertFromString(raw.partial.choice)

    // Convert DAML-LF values to ledger API values.
    // In verbose mode, this involves loading DAML-LF packages and filling in missing type information.
    for {
      choiceArgument <- toApiValue(
        value = exercise.argument,
        verbose = verbose,
        attribute = "exercise argument",
        enrich = value => enricher.enrichChoiceArgument(templateId, choiceName, value.value),
      )
      exerciseResult <- exercise.result match {
        case Some(result) =>
          toApiValue(
            value = result,
            verbose = verbose,
            attribute = "exercise result",
            enrich = value => enricher.enrichChoiceResult(templateId, choiceName, value.value),
          ).map(Some(_))
        case None => Future.successful(None)
      }
    } yield {
      raw.partial.copy(
        choiceArgument = Some(choiceArgument),
        exerciseResult = exerciseResult,
      )
    }
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
        contractConfiguration: caching.SizedCache.Configuration,
    ): Cache =
      Cache(
        events = EventCache.newInstance(eventConfiguration),
        contracts = ContractCache.newInstance(contractConfiguration),
      )

    def newInstrumentedInstance(
        eventConfiguration: caching.SizedCache.Configuration,
        contractConfiguration: caching.SizedCache.Configuration,
        metrics: Metrics,
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
        metrics: Metrics,
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
        metrics: Metrics,
    ): ContractCache =
      caching.SizedCache.from(
        configuration = configuration,
        metrics = metrics.daml.index.db.translation.cache,
      )

    final case class Key(contractId: ContractId)

    final case class Value(argument: LfValue)
  }
}
