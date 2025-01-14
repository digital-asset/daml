// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import cats.implicits.toTraverseOps
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.event.{ArchivedEvent, CreatedEvent, ExercisedEvent, InterfaceView}
import com.daml.ledger.api.v2.value.{Record as ApiRecord, Value as ApiValue}
import com.daml.metrics.Timed
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.services.{ErrorCause, RejectionGenerators}
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawArchivedEvent,
  RawCreatedEvent,
  RawExercisedEvent,
}
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation.ApiContractData
import com.digitalasset.canton.platform.store.serialization.{Compression, ValueSerializer}
import com.digitalasset.canton.platform.{
  ContractId,
  Create,
  Exercise,
  Identifier as LfIdentifier,
  PackageId as LfPackageId,
  Value as LfValue,
}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.engine as LfEngine
import com.digitalasset.daml.lf.engine.{Engine, ValueEnricher}
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.VersionedValue
import com.google.protobuf.ByteString
import com.google.rpc.Status
import com.google.rpc.status.Status as ProtoStatus
import io.grpc.Status.Code

import java.io.ByteArrayInputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

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
  def serialize(create: Create): (Array[Byte], Option[Array[Byte]])

  /** Returns (choice argument, exercise result, contract key) */
  def serialize(
      exercise: Exercise
  ): (Array[Byte], Option[Array[Byte]], Option[Array[Byte]])
}

final class LfValueTranslation(
    metrics: LedgerApiServerMetrics,
    // Note: LfValueTranslation is used by JdbcLedgerDao for both serialization and deserialization.
    // Sometimes the JdbcLedgerDao is used in a way that it never needs to deserialize data in verbose mode
    // (e.g., the indexer, or some tests). In this case, the engine is not required.
    engineO: Option[Engine],
    loadPackage: (
        LfPackageId,
        LoggingContextWithTrace,
    ) => Future[Option[com.digitalasset.daml.lf.archive.DamlLf.Archive]],
    val loggerFactory: NamedLoggerFactory,
) extends LfValueSerialization
    with NamedLogging {

  private val enricherO = engineO.map(new ValueEnricher(_))

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
        value = k.map(_.value),
        errorContext = cantSerialize(attribute = "key", forContract = c.coid),
      )
    )

  private def serializeNullableKeyOrThrow(e: Exercise): Option[Array[Byte]] =
    e.versionedKey.map(k =>
      ValueSerializer.serializeValue(
        value = k.map(_.value),
        errorContext = cantSerialize(attribute = "key", forContract = e.targetCoid),
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

  override def serialize(
      contractId: ContractId,
      contractArgument: VersionedValue,
  ): Array[Byte] =
    serializeCreateArgOrThrow(contractId, contractArgument)

  override def serialize(create: Create): (Array[Byte], Option[Array[Byte]]) =
    serializeCreateArgOrThrow(create) -> serializeNullableKeyOrThrow(create)

  override def serialize(
      exercise: Exercise
  ): (Array[Byte], Option[Array[Byte]], Option[Array[Byte]]) =
    (
      serializeExerciseArgOrThrow(exercise),
      serializeNullableExerciseResultOrThrow(exercise),
      serializeNullableKeyOrThrow(exercise),
    )

  private[this] def consumeEnricherResult[V](
      result: LfEngine.Result[V]
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[V] =
    result match {
      case LfEngine.ResultDone(r) => Future.successful(r)
      case LfEngine.ResultError(e) => Future.failed(new RuntimeException(e.message))
      case LfEngine.ResultNeedPackage(packageId, resume) =>
        packageLoader
          .loadPackage(
            packageId = packageId,
            delegate = packageId => loadPackage(packageId, loggingContext),
            metric = metrics.index.db.translation.getLfPackage,
          )
          .flatMap(pkgO => consumeEnricherResult(resume(pkgO)))
      case result =>
        Future.failed(new RuntimeException(s"Unexpected ValueEnricher result: $result"))
    }

  def enrichVersionedTransaction(versionedTransaction: VersionedTransaction)(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[VersionedTransaction] =
    consumeEnricherResult(enricher.enrichVersionedTransaction(versionedTransaction))

  def toApiValue(
      value: LfValue,
      verbose: Boolean,
      attribute: => String,
      enrich: LfValue => LfEngine.Result[com.digitalasset.daml.lf.value.Value],
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
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

  private def decompressAndDeserialize(algorithm: Compression.Algorithm, value: Array[Byte]) =
    ValueSerializer.deserializeValue(algorithm.decompress(new ByteArrayInputStream(value)))

  def enricher: ValueEnricher =
    enricherO.getOrElse(
      sys.error(
        "LfValueTranslation used to deserialize values in verbose mode without an Engine"
      )
    )

  def engine: Engine =
    engineO.getOrElse(
      sys.error(
        "LfValueTranslation used to deserialize values in verbose mode without an Engine"
      )
    )

  def deserializeRaw(
      verbose: Boolean
  )(
      rawExercisedEvent: RawExercisedEvent
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[ExercisedEvent] =
    for {
      // Deserialize contract argument and contract key
      // This returns the values in Daml-LF format.
      exerciseArgument <- Future(
        decompressAndDeserialize(
          Compression.Algorithm
            .assertLookup(rawExercisedEvent.exerciseArgumentCompression),
          rawExercisedEvent.exerciseArgument,
        )
      )
      exerciseResult <- Future(
        rawExercisedEvent.exerciseResult.map(
          decompressAndDeserialize(
            Compression.Algorithm
              .assertLookup(rawExercisedEvent.exerciseResultCompression),
            _,
          )
        )
      )
      Ref.QualifiedChoiceName(interfaceId, choiceName) =
        Ref.QualifiedChoiceName.assertFromString(rawExercisedEvent.exerciseChoice)
      // Convert Daml-LF values to ledger API values.
      // In verbose mode, this involves loading Daml-LF packages and filling in missing type information.
      choiceArgument <- toApiValue(
        value = exerciseArgument,
        verbose = verbose,
        attribute = "exercise argument",
        enrich = value =>
          enricher.enrichChoiceArgument(
            rawExercisedEvent.templateId,
            interfaceId,
            choiceName,
            value.unversioned,
          ),
      )
      exerciseResult <- exerciseResult match {
        case Some(result) =>
          toApiValue(
            value = result,
            verbose = verbose,
            attribute = "exercise result",
            enrich = value =>
              enricher.enrichChoiceResult(
                rawExercisedEvent.templateId,
                interfaceId,
                choiceName,
                value.unversioned,
              ),
          ).map(Some(_))
        case None => Future.successful(None)
      }
    } yield ExercisedEvent(
      offset = rawExercisedEvent.offset,
      nodeId = rawExercisedEvent.nodeId,
      contractId = rawExercisedEvent.contractId,
      templateId = Some(
        LfEngineToApi.toApiIdentifier(rawExercisedEvent.templateId)
      ),
      interfaceId = interfaceId.map(
        LfEngineToApi.toApiIdentifier
      ),
      choice = choiceName,
      choiceArgument = Some(choiceArgument),
      actingParties = rawExercisedEvent.exerciseActors,
      consuming = rawExercisedEvent.exerciseConsuming,
      witnessParties = rawExercisedEvent.witnessParties.toSeq,
      childNodeIds = rawExercisedEvent.exerciseChildNodeIds,
      exerciseResult = exerciseResult,
      packageName = rawExercisedEvent.packageName,
    )

  def deserializeRaw(
      rawArchivedEvent: RawArchivedEvent
  ): ArchivedEvent =
    ArchivedEvent(
      offset = rawArchivedEvent.offset,
      nodeId = rawArchivedEvent.nodeId,
      contractId = rawArchivedEvent.contractId,
      templateId = Some(
        LfEngineToApi.toApiIdentifier(rawArchivedEvent.templateId)
      ),
      witnessParties = rawArchivedEvent.witnessParties.toSeq,
      packageName = rawArchivedEvent.packageName,
    )

  def deserializeRaw(
      eventProjectionProperties: EventProjectionProperties
  )(
      rawCreatedEvent: RawCreatedEvent
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[CreatedEvent] = {
    def getFatContractInstance(
        createArgument: VersionedValue,
        createKey: Option[VersionedValue],
    ): Either[String, FatContractInstance] =
      for {
        contractId <- ContractId.fromString(rawCreatedEvent.contractId)
        signatories <- rawCreatedEvent.signatories.toList.traverse(Party.fromString).map(_.toSet)
        observers <- rawCreatedEvent.observers.toList.traverse(Party.fromString).map(_.toSet)
        maintainers <- rawCreatedEvent.createKeyMaintainers.toList
          .traverse(Party.fromString)
          .map(_.toSet)
        globalKey <- createKey
          .traverse(key =>
            GlobalKey
              .build(rawCreatedEvent.templateId, key.unversioned, rawCreatedEvent.packageName)
              .left
              .map(_.msg)
          )
      } yield FatContractInstance.fromCreateNode(
        Node.Create(
          coid = contractId,
          templateId = rawCreatedEvent.templateId,
          packageName = rawCreatedEvent.packageName,
          packageVersion = rawCreatedEvent.packageVersion,
          arg = createArgument.unversioned,
          signatories = signatories,
          stakeholders = signatories ++ observers,
          keyOpt = globalKey.map(GlobalKeyWithMaintainers(_, maintainers)),
          version = createArgument.version,
        ),
        createTime = rawCreatedEvent.ledgerEffectiveTime,
        cantonData = Bytes.fromByteArray(rawCreatedEvent.driverMetadata),
      )

    for {
      createKey <- Future(
        rawCreatedEvent.createKeyValue
          .map(
            decompressAndDeserialize(
              Compression.Algorithm
                .assertLookup(rawCreatedEvent.createKeyValueCompression),
              _,
            )
          )
      )
      createArgument <- Future(
        decompressAndDeserialize(
          Compression.Algorithm
            .assertLookup(rawCreatedEvent.createArgumentCompression),
          rawCreatedEvent.createArgument,
        )
      )
      apiContractData <- toApiContractData(
        value = createArgument,
        key = createKey,
        templateId = rawCreatedEvent.templateId,
        witnesses = rawCreatedEvent.witnessParties,
        eventProjectionProperties = eventProjectionProperties,
        fatContractInstance = getFatContractInstance(createArgument, createKey),
      )
    } yield CreatedEvent(
      offset = rawCreatedEvent.offset,
      nodeId = rawCreatedEvent.nodeId,
      contractId = rawCreatedEvent.contractId,
      templateId = Some(
        LfEngineToApi.toApiIdentifier(rawCreatedEvent.templateId)
      ),
      contractKey = apiContractData.contractKey,
      createArguments = apiContractData.createArguments,
      createdEventBlob = apiContractData.createdEventBlob.getOrElse(ByteString.EMPTY),
      interfaceViews = apiContractData.interfaceViews,
      witnessParties = rawCreatedEvent.witnessParties.toList,
      signatories = rawCreatedEvent.signatories.toList,
      observers = rawCreatedEvent.observers.toList,
      createdAt = Some(TimestampConversion.fromLf(rawCreatedEvent.ledgerEffectiveTime)),
      packageName = rawCreatedEvent.packageName,
    )
  }

  def toApiContractData(
      value: LfValue,
      key: Option[VersionedValue],
      templateId: LfIdentifier,
      witnesses: Set[String],
      eventProjectionProperties: EventProjectionProperties,
      fatContractInstance: => Either[String, FatContractInstance],
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[ApiContractData] = {
    val renderResult =
      eventProjectionProperties.render(witnesses, templateId)
    val verbose = eventProjectionProperties.verbose
    def asyncContractArguments = condFuture(renderResult.contractArguments)(
      enrichAsync(verbose, value.unversioned, enricher.enrichContract(templateId, _))
        .map(toContractArgumentApi(verbose))
    )
    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    def asyncContractKey = condFuture(renderResult.contractArguments && key.isDefined)(
      enrichAsync(verbose, key.get.unversioned, enricher.enrichContractKey(templateId, _))
        .map(toContractKeyApi(verbose))
    )
    def asyncInterfaceViews =
      MonadUtil.sequentialTraverse(renderResult.interfaces.toList)(interfaceId =>
        computeInterfaceView(
          templateId,
          value.unversioned,
          interfaceId,
        ).flatMap(toInterfaceView(eventProjectionProperties.verbose, interfaceId))
      )

    def asyncCreatedEventBlob = condFuture(renderResult.createdEventBlob) {
      (for {
        fatInstance <- fatContractInstance
        encoded <- TransactionCoder
          .encodeFatContractInstance(fatInstance)
          .left
          .map(_.errorMessage)
      } yield encoded).fold(
        err => Future.failed(new RuntimeException(s"Cannot serialize createdEventBlob: $err")),
        Future.successful,
      )
    }

    for {
      contractArguments <- asyncContractArguments
      createdEventBlob <- asyncCreatedEventBlob
      contractKey <- asyncContractKey
      interfaceViews <- asyncInterfaceViews
    } yield ApiContractData(
      createArguments = contractArguments,
      createdEventBlob = createdEventBlob,
      contractKey = contractKey,
      interfaceViews = interfaceViews,
    )
  }

  private def toInterfaceView(verbose: Boolean, interfaceId: Identifier)(
      result: Either[Status, Versioned[Value]]
  )(implicit ec: ExecutionContext, loggingContext: LoggingContextWithTrace): Future[InterfaceView] =
    result match {
      case Right(versionedValue) =>
        enrichAsync(verbose, versionedValue.unversioned, enricher.enrichView(interfaceId, _))
          .map(toInterfaceViewApi(verbose, interfaceId))
      case Left(errorStatus) =>
        Future.successful(
          InterfaceView(
            interfaceId = Some(LfEngineToApi.toApiIdentifier(interfaceId)),
            viewStatus = Some(ProtoStatus.fromJavaProto(errorStatus)),
            viewValue = None,
          )
        )
    }

  private def condFuture[T](cond: Boolean)(f: => Future[T])(implicit
      ec: ExecutionContext
  ): Future[Option[T]] =
    if (cond) f.map(Some(_)) else Future.successful(None)

  private def enrichAsync(verbose: Boolean, value: Value, enrich: Value => LfEngine.Result[Value])(
      implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[Value] =
    condFuture(verbose)(
      Future(enrich(value)).flatMap(consumeEnricherResult)
    ).map(_.getOrElse(value))

  private def toApi[T](
      verbose: Boolean,
      lfEngineToApiFunction: (Boolean, Value) => Either[String, T],
      attribute: String,
  )(value: Value): T =
    LfEngineToApi.assertOrRuntimeEx(
      failureContext = s"attempting to serialize $attribute to API record",
      lfEngineToApiFunction(verbose, value),
    )

  private def toContractArgumentApi(verbose: Boolean)(value: Value): ApiRecord =
    toApi(verbose, LfEngineToApi.lfValueToApiRecord, "create argument")(value)

  private def toContractKeyApi(verbose: Boolean)(value: Value): ApiValue =
    toApi(verbose, LfEngineToApi.lfValueToApiValue, "create key")(value)

  private def toInterfaceViewApi(verbose: Boolean, interfaceId: Identifier)(value: Value) =
    InterfaceView(
      interfaceId = Some(LfEngineToApi.toApiIdentifier(interfaceId)),
      viewStatus = Some(ProtoStatus.of(Code.OK.value(), "", Seq.empty)),
      viewValue = Some(toApi(verbose, LfEngineToApi.lfValueToApiRecord, "interface view")(value)),
    )

  private def computeInterfaceView(
      templateId: LfIdentifier,
      value: com.digitalasset.daml.lf.value.Value,
      interfaceId: LfIdentifier,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[Either[Status, Versioned[Value]]] = Timed.future(
    metrics.index.lfValue.computeInterfaceView, {
      implicit val errorLogger: ContextualizedErrorLogger =
        ErrorLoggingContext(logger, loggingContext)

      def goAsync(
          res: LfEngine.Result[Versioned[Value]]
      ): Future[Either[Status, Versioned[Value]]] =
        res match {
          case LfEngine.ResultDone(x) =>
            Future.successful(Right(x))

          case LfEngine.ResultError(err) =>
            err
              .pipe(ErrorCause.DamlLf.apply)
              .pipe(RejectionGenerators.commandExecutorError)
              .pipe(_.asGrpcStatus)
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
                metric = metrics.index.db.translation.getLfPackage,
              )
              .map(resume)
              .flatMap(goAsync)

          case LfEngine.ResultInterruption(continue, _) =>
            goAsync(continue())

          case LfEngine.ResultNeedUpgradeVerification(_, _, _, _, _) =>
            Future.failed(new IllegalStateException("View computation must be a pure function"))

          case LfEngine.ResultPrefetch(_, resume) =>
            goAsync(resume())
        }

      Future(engine.computeInterfaceView(templateId, value, interfaceId))
        .flatMap(goAsync)
    },
  )
}

object LfValueTranslation {

  final case class ApiContractData(
      createArguments: Option[ApiRecord],
      createdEventBlob: Option[ByteString],
      contractKey: Option[ApiValue],
      interfaceViews: Seq[InterfaceView],
  )
}
