// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.ByteArrayInputStream
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.{ErrorCause, RejectionGenerators}
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent, InterfaceView}
import com.daml.ledger.api.v1.value.{
  Identifier => ApiIdentifier,
  Record => ApiRecord,
  Value => ApiValue,
}
import com.daml.lf.engine.{Engine, ValueEnricher}
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.Versioned
import com.daml.lf.value.Value
import com.daml.lf.value.Value.VersionedValue
import com.daml.lf.{engine => LfEngine}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.{
  ContractId,
  Create,
  Exercise,
  ChoiceName => LfChoiceName,
  DottedName => LfDottedName,
  Identifier => LfIdentifier,
  ModuleName => LfModuleName,
  PackageId => LfPackageId,
  QualifiedName => LfQualifiedName,
  Value => LfValue,
}
import com.daml.platform.packages.DeduplicatingPackageLoader
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.LfValueTranslationCache
import com.daml.platform.store.dao.EventProjectionProperties
import com.daml.platform.store.serialization.{Compression, ValueSerializer}
import io.grpc.Status
import io.grpc.Status.Code

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._

/** Serializes and deserializes Daml-Lf values and events.
  *
  *  Deserializing values in verbose mode involves loading packages in order to fill in missing type information.
  *  That's why these methods return Futures, while the serialization methods are synchronous.
  */
trait LfValueSerialization {
  def serialize(
      contractId: ContractId,
      contractArgument: VersionedValue,
  ): Array[Byte]

  /** Returns (contract argument, contract key) */
  def serialize(
      eventId: EventId,
      create: Create,
  ): (Array[Byte], Option[Array[Byte]])

  /** Returns (choice argument, exercise result, contract key) */
  def serialize(
      eventId: EventId,
      exercise: Exercise,
  ): (Array[Byte], Option[Array[Byte]], Option[Array[Byte]])

  def deserialize[E](
      raw: Raw.Created[E],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[CreatedEvent]

  def deserialize(
      raw: Raw.TreeEvent.Exercised,
      verbose: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[ExercisedEvent]
}

final class LfValueTranslation(
    val cache: LfValueTranslationCache.Cache,
    metrics: Metrics,
    engineO: Option[Engine],
    loadPackage: (
        LfPackageId,
        LoggingContext,
    ) => Future[Option[com.daml.daml_lf_dev.DamlLf.Archive]],
) extends LfValueSerialization {

  private val logger = ContextualizedLogger.get(this.getClass)

  private[this] val packageLoader = new DeduplicatingPackageLoader()

  private def cantSerialize(attribute: String, forContract: ContractId): String =
    s"Cannot serialize $attribute for ${forContract.coid}"

  private def serializeCreateArgOrThrow(
      contractId: ContractId,
      arg: VersionedValue,
  ): Array[Byte] =
    ValueSerializer.serializeValue(
      value = arg,
      errorContext = cantSerialize(attribute = "create argument", forContract = contractId),
    )

  private def serializeCreateArgOrThrow(c: Create): Array[Byte] =
    serializeCreateArgOrThrow(c.coid, c.versionedArg)

  private def serializeNullableKeyOrThrow(c: Create): Option[Array[Byte]] =
    c.versionedKey.map(k =>
      ValueSerializer.serializeValue(
        value = k.map(_.key),
        errorContext = cantSerialize(attribute = "key", forContract = c.coid),
      )
    )

  private def serializeNullableKeyOrThrow(e: Exercise): Option[Array[Byte]] = {
    e.versionedKey.map(k =>
      ValueSerializer.serializeValue(
        value = k.map(_.key),
        errorContext = cantSerialize(attribute = "key", forContract = e.targetCoid),
      )
    )
  }

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

  override def serialize(
      contractId: ContractId,
      contractArgument: VersionedValue,
  ): Array[Byte] = {
    cache.contracts.put(
      key = LfValueTranslationCache.ContractCache.Key(contractId),
      value = LfValueTranslationCache.ContractCache.Value(contractArgument),
    )
    serializeCreateArgOrThrow(contractId, contractArgument)
  }

  override def serialize(eventId: EventId, create: Create): (Array[Byte], Option[Array[Byte]]) = {
    cache.events.put(
      key = LfValueTranslationCache.EventCache.Key(eventId),
      value = LfValueTranslationCache.EventCache.Value
        .Create(create.versionedArg, create.versionedKey.map(_.map(_.key))),
    )
    cache.contracts.put(
      key = LfValueTranslationCache.ContractCache.Key(create.coid),
      value = LfValueTranslationCache.ContractCache.Value(create.versionedArg),
    )
    (serializeCreateArgOrThrow(create), serializeNullableKeyOrThrow(create))
  }

  override def serialize(
      eventId: EventId,
      exercise: Exercise,
  ): (Array[Byte], Option[Array[Byte]], Option[Array[Byte]]) = {
    cache.events.put(
      key = LfValueTranslationCache.EventCache.Key(eventId),
      value = LfValueTranslationCache.EventCache.Value
        .Exercise(exercise.versionedChosenValue, exercise.versionedExerciseResult),
    )
    (
      serializeExerciseArgOrThrow(exercise),
      serializeNullableExerciseResultOrThrow(exercise),
      serializeNullableKeyOrThrow(exercise),
    )
  }

  private[this] def consumeEnricherResult[V](
      result: LfEngine.Result[V]
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[V] = {
    result match {
      case LfEngine.ResultDone(r) => Future.successful(r)
      case LfEngine.ResultError(e) => Future.failed(new RuntimeException(e.message))
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

  def toApiValue(
      value: LfValue,
      verbose: Boolean,
      attribute: => String,
      enrich: LfValue => LfEngine.Result[com.daml.lf.value.Value],
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[ApiValue] = for {
    enrichedValue <-
      if (verbose)
        consumeEnricherResult(enrich(value))
      else
        Future.successful(value.unversioned)
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

  case class ApiContractData(
      createArguments: Option[ApiRecord],
      contractKey: Option[ApiValue],
      interfaceViews: Seq[InterfaceView],
  )
  def toApiContractData(
      value: LfValue,
      key: Option[VersionedValue],
      templateId: LfIdentifier,
      witnesses: Seq[String],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[ApiContractData] = {

    val renderContractArguments =
      eventProjectionProperties.renderContractArguments(witnesses, templateId)

    val renderInterfaces = eventProjectionProperties.renderInterfaces(witnesses, templateId)

    def condFuture[T](cond: Boolean)(f: => Future[T]): Future[Option[T]] =
      if (cond) f.map(Some(_)) else Future.successful(None)
    def enrichAsync(enrich: Value => LfEngine.Result[Value])(value: Value): Future[Value] =
      condFuture(eventProjectionProperties.verbose)(
        Future(enrich(value)).flatMap(consumeEnricherResult)
      ).map(_.getOrElse(value))
    def toApi[T](
        lfEngineToApiFunction: (Boolean, Value) => Either[String, T],
        attribute: => String,
    )(value: Value): T =
      LfEngineToApi.assertOrRuntimeEx(
        // TODO DPP-1068: this is not always right
        failureContext = s"attempting to deserialize persisted $attribute to record",
        lfEngineToApiFunction(eventProjectionProperties.verbose, value),
      )
    val asyncContractAguments = condFuture(renderContractArguments)(
      enrichAsync(enricher.enrichContract(templateId, _))(value.unversioned)
        .map(toApi(LfEngineToApi.lfValueToApiRecord, "create argument"))
    )
    val asyncContractKey = condFuture(renderContractArguments && key.isDefined)(
      enrichAsync(
        enricher.enrichContractKey(templateId, _)
      )(
        key.get.unversioned
      )
        .map(toApi(LfEngineToApi.lfValueToApiValue, "create key"))
    )
    val asyncInterfaceViews = Future.traverse(renderInterfaces.toSeq)(interfaceId =>
      computeInterfaceView(
        templateId,
        value.unversioned,
        interfaceId,
      ).flatMap {
        case Right(versionedValue) =>
          enrichAsync(enricher.enrichView(interfaceId, _))(versionedValue.unversioned)
            .map(toApi(LfEngineToApi.lfValueToApiRecord, "interface view"))
            .map(Some(_))
            .map(
              InterfaceView(
                interfaceId = Some(LfEngineToApi.toApiIdentifier(interfaceId)),
                viewStatus = Some(com.google.rpc.status.Status.of(Code.OK.value(), "", Seq.empty)),
                _,
              )
            )

        case Left(errorStatus) =>
          Future.successful(
            InterfaceView(
              interfaceId = Some(LfEngineToApi.toApiIdentifier(interfaceId)),
              // TODO DPP-1068: is it okay to convert from io.grpc.Status to com.google.rpc.status.Status this way?
              viewStatus = Some(
                com.google.rpc.status.Status
                  .of(errorStatus.getCode.value(), errorStatus.getDescription, Seq.empty)
              ),
              viewValue = None,
            )
          )
      }
    )

    for {
      contractArguments <- asyncContractAguments
      contractKey <- asyncContractKey
      interfaceViews <- asyncInterfaceViews
    } yield ApiContractData(
      createArguments = contractArguments,
      contractKey = contractKey,
      interfaceViews = interfaceViews,
    )

  }

  private def computeInterfaceView(
      templateId: LfIdentifier,
      value: com.daml.lf.value.Value,
      interfaceId: LfIdentifier,
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Future[Either[Status, Versioned[Value]]] = {
    def goAsync(res: LfEngine.Result[Versioned[Value]]): Future[Either[Status, Versioned[Value]]] =
      res match {
        case LfEngine.ResultDone(x) =>
          Future.successful(Right(x))

        case LfEngine.ResultError(err) =>
          implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
            new DamlContextualizedErrorLogger(logger, loggingContext, None)
          err
            .pipe(ErrorCause.DamlLf)
            .pipe(RejectionGenerators.commandExecutorError)
            .getStatus
            .pipe(Left.apply)
            .pipe(Future.successful)

        // Note: the compiler should enforce that the computation is a pure function,
        // ResultNeedContract and ResultNeedKey should never appear in the result.
        case LfEngine.ResultNeedContract(_, _) =>
          Future.failed(new IllegalStateException("View computation must be a pure function"))

        case LfEngine.ResultNeedKey(_, _) =>
          Future.failed(new IllegalStateException("View computation must be a pure function"))

        case LfEngine.ResultNeedPackage(packageId, resume) =>
          packageLoader
            .loadPackage(
              packageId = packageId,
              delegate = packageId => loadPackage(packageId, loggingContext),
              metric = metrics.daml.index.db.translation.getLfPackage,
            )
            .map(resume)
            .flatMap(goAsync)
      }
    Future(engineO.get.computeInterfaceView(templateId, value, interfaceId))
      .flatMap(goAsync)
  }

  private[this] def apiIdentifierToDamlLfIdentifier(id: ApiIdentifier): LfIdentifier =
    LfIdentifier(
      LfPackageId.assertFromString(id.packageId),
      LfQualifiedName(
        LfModuleName.assertFromString(id.moduleName),
        LfDottedName.assertFromString(id.entityName),
      ),
    )

  private def eventKey(s: String) =
    LfValueTranslationCache.EventCache.Key(EventId.assertFromString(s))

  private def decompressAndDeserialize(algorithm: Compression.Algorithm, value: Array[Byte]) =
    ValueSerializer.deserializeValue(algorithm.decompress(new ByteArrayInputStream(value)))

  private val enricherO = engineO.map(new ValueEnricher(_))

  def enricher: ValueEnricher = {
    // Note: LfValueTranslation is used by JdbcLedgerDao for both serialization and deserialization.
    // Sometimes the JdbcLedgerDao is used in a way that it never needs to deserialize data in verbose mode
    // (e.g., the indexer, or some tests). In this case, the enricher is not required.
    enricherO.getOrElse(
      sys.error(
        "LfValueTranslation used to deserialize values in verbose mode without a ValueEnricher"
      )
    )
  }

  override def deserialize[E](
      raw: Raw.Created[E],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[CreatedEvent] = {
    val templateId: LfIdentifier = apiIdentifierToDamlLfIdentifier(raw.partial.templateId.get)

    for {
      create <- Future(
        // Load the deserialized contract argument and contract key from the cache
        // This returns the values in Daml-LF format.
        cache.events
          .getIfPresent(eventKey(raw.partial.eventId))
          .getOrElse(
            LfValueTranslationCache.EventCache.Value.Create(
              argument =
                decompressAndDeserialize(raw.createArgumentCompression, raw.createArgument),
              key =
                raw.createKeyValue.map(decompressAndDeserialize(raw.createKeyValueCompression, _)),
            )
          )
          .assertCreate()
      )
      apiContractData <- toApiContractData(
        value = create.argument,
        key = create.key,
        templateId = templateId,
        witnesses = raw.partial.witnessParties,
        eventProjectionProperties = eventProjectionProperties,
      )
    } yield raw.partial.copy(
      createArguments = apiContractData.createArguments,
      contractKey = apiContractData.contractKey,
      interfaceViews = apiContractData.interfaceViews,
    )
  }

  override def deserialize(
      raw: Raw.TreeEvent.Exercised,
      verbose: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[ExercisedEvent] = {
    // Load the deserialized choice argument and choice result from the cache
    // This returns the values in Daml-LF format.
    val exercise =
      cache.events
        .getIfPresent(eventKey(raw.partial.eventId))
        .getOrElse(
          LfValueTranslationCache.EventCache.Value.Exercise(
            argument =
              decompressAndDeserialize(raw.exerciseArgumentCompression, raw.exerciseArgument),
            result =
              raw.exerciseResult.map(decompressAndDeserialize(raw.exerciseResultCompression, _)),
          )
        )
        .assertExercise()

    lazy val temlateId: LfIdentifier = apiIdentifierToDamlLfIdentifier(raw.partial.templateId.get)
    lazy val interfaceId: Option[LfIdentifier] =
      raw.partial.interfaceId.map(apiIdentifierToDamlLfIdentifier)
    lazy val choiceName: LfChoiceName = LfChoiceName.assertFromString(raw.partial.choice)

    // Convert Daml-LF values to ledger API values.
    // In verbose mode, this involves loading Daml-LF packages and filling in missing type information.
    for {
      choiceArgument <- toApiValue(
        value = exercise.argument,
        verbose = verbose,
        attribute = "exercise argument",
        enrich = value =>
          enricher.enrichChoiceArgument(temlateId, interfaceId, choiceName, value.unversioned),
      )
      exerciseResult <- exercise.result match {
        case Some(result) =>
          toApiValue(
            value = result,
            verbose = verbose,
            attribute = "exercise result",
            enrich = value =>
              enricher.enrichChoiceResult(temlateId, interfaceId, choiceName, value.unversioned),
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
