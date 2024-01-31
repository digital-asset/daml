// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import cats.implicits.toTraverseOps
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent, InterfaceView}
import com.daml.ledger.api.v1.value.{
  Identifier as ApiIdentifier,
  Record as ApiRecord,
  Value as ApiValue,
}
import com.daml.lf.data.Bytes
import com.daml.lf.data.Ref.{DottedName, Identifier, PackageId, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Engine, ValueEnricher}
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.*
import com.daml.lf.value.Value
import com.daml.lf.value.Value.VersionedValue
import com.daml.lf.engine as LfEngine
import com.daml.metrics.Timed
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.services.{ErrorCause, RejectionGenerators}
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawCreatedEvent
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation.ApiContractData
import com.digitalasset.canton.platform.store.serialization.{Compression, ValueSerializer}
import com.digitalasset.canton.platform.{
  ChoiceName as LfChoiceName,
  ContractId,
  Create,
  DottedName as LfDottedName,
  Exercise,
  Identifier as LfIdentifier,
  ModuleName as LfModuleName,
  PackageId as LfPackageId,
  QualifiedName as LfQualifiedName,
  Value as LfValue,
}
import com.digitalasset.canton.serialization.ProtoConverter.InstantConverter
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp as ApiTimestamp
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
      eventId: EventId,
      exercise: Exercise,
  ): (Array[Byte], Option[Array[Byte]], Option[Array[Byte]])

  def deserialize[E](
      raw: Raw.Created[E],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[CreatedEvent]

  def deserialize(
      raw: Raw.TreeEvent.Exercised,
      verbose: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[ExercisedEvent]
}

final class LfValueTranslation(
    metrics: Metrics,
    // Note: LfValueTranslation is used by JdbcLedgerDao for both serialization and deserialization.
    // Sometimes the JdbcLedgerDao is used in a way that it never needs to deserialize data in verbose mode
    // (e.g., the indexer, or some tests). In this case, the engine is not required.
    engineO: Option[Engine],
    loadPackage: (
        LfPackageId,
        LoggingContextWithTrace,
    ) => Future[Option[com.daml.daml_lf_dev.DamlLf.Archive]],
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

  private def serializeNullableKeyOrThrow(e: Exercise): Option[Array[Byte]] = {
    e.versionedKey.map(k =>
      ValueSerializer.serializeValue(
        value = k.map(_.value),
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
  ): Array[Byte] =
    serializeCreateArgOrThrow(contractId, contractArgument)

  override def serialize(create: Create): (Array[Byte], Option[Array[Byte]]) =
    serializeCreateArgOrThrow(create) -> serializeNullableKeyOrThrow(create)

  override def serialize(
      eventId: EventId,
      exercise: Exercise,
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
  ): Future[V] = {
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
  }

  def toApiValue(
      value: LfValue,
      verbose: Boolean,
      attribute: => String,
      enrich: LfValue => LfEngine.Result[com.daml.lf.value.Value],
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

  private[this] def apiIdentifierToDamlLfIdentifier(id: ApiIdentifier): LfIdentifier =
    LfIdentifier(
      LfPackageId.assertFromString(id.packageId),
      LfQualifiedName(
        LfModuleName.assertFromString(id.moduleName),
        LfDottedName.assertFromString(id.entityName),
      ),
    )

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

  override def deserialize[E](
      raw: Raw.Created[E],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[CreatedEvent] = {
    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    lazy val templateId: LfIdentifier = apiIdentifierToDamlLfIdentifier(raw.partial.templateId.get)

    def getFatContractInstance(
        createArgument: VersionedValue,
        createKey: Option[VersionedValue],
    ): Option[Either[String, FatContractInstance]] =
      raw.driverMetadata.filter(_.nonEmpty).map { driverMetadataBytes =>
        for {
          contractId <- ContractId.fromString(raw.partial.contractId)
          apiTemplateId <- raw.partial.templateId
            .fold[Either[String, ApiIdentifier]](Left("missing templateId"))(Right(_))
          packageId <- PackageId.fromString(apiTemplateId.packageId)
          moduleName <- DottedName.fromString(apiTemplateId.moduleName)
          entityName <- DottedName.fromString(apiTemplateId.entityName)
          templateId = Identifier(packageId, LfQualifiedName(moduleName, entityName))
          signatories <- raw.partial.signatories.traverse(Party.fromString).map(_.toSet)
          observers <- raw.partial.observers.traverse(Party.fromString).map(_.toSet)
          maintainers <- raw.createKeyMaintainers.toList.traverse(Party.fromString).map(_.toSet)
          globalKey <- createKey
            .traverse(key =>
              GlobalKey
                .build(templateId, key.unversioned, Util.sharedKey(key.version))
                .left
                .map(_.msg)
            )
          apiCreatedAt <- raw.partial.createdAt
            .fold[Either[String, ApiTimestamp]](Left("missing createdAt"))(Right(_))
          instant <- InstantConverter.fromProtoPrimitive(apiCreatedAt).left.map(_.message)
          createdAt <- Timestamp.fromInstant(instant)
        } yield FatContractInstance.fromCreateNode(
          Node.Create(
            coid = contractId,
            templateId = templateId,
            arg = createArgument.unversioned,
            agreementText = raw.partial.agreementText.getOrElse(""),
            signatories = signatories,
            stakeholders = signatories ++ observers,
            keyOpt = globalKey.map(GlobalKeyWithMaintainers(_, maintainers)),
            version = createArgument.version,
          ),
          createTime = createdAt,
          cantonData = Bytes.fromByteArray(driverMetadataBytes),
        )
      }

    for {
      createKey <- Future(
        raw.createKeyValue.map(decompressAndDeserialize(raw.createKeyValueCompression, _))
      )
      createArgument <- Future(
        decompressAndDeserialize(raw.createArgumentCompression, raw.createArgument)
      )
      apiContractData <- toApiContractData(
        value = createArgument,
        key = createKey,
        templateId = templateId,
        witnesses = raw.partial.witnessParties.toSet,
        eventProjectionProperties = eventProjectionProperties,
        fatContractInstance = getFatContractInstance(createArgument, createKey),
      )
    } yield raw.partial.copy(
      createArguments = apiContractData.createArguments,
      contractKey = apiContractData.contractKey,
      interfaceViews = apiContractData.interfaceViews,
      createdEventBlob = apiContractData.createdEventBlob.getOrElse(ByteString.EMPTY),
    )
  }

  override def deserialize(
      raw: Raw.TreeEvent.Exercised,
      verbose: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[ExercisedEvent] = {
    // Deserialize contract argument and contract key
    // This returns the values in Daml-LF format.
    val exerciseArgument =
      decompressAndDeserialize(raw.exerciseArgumentCompression, raw.exerciseArgument)
    val exerciseResult =
      raw.exerciseResult.map(decompressAndDeserialize(raw.exerciseResultCompression, _))

    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    lazy val templateId: LfIdentifier = apiIdentifierToDamlLfIdentifier(raw.partial.templateId.get)
    lazy val interfaceId: Option[LfIdentifier] =
      raw.partial.interfaceId.map(apiIdentifierToDamlLfIdentifier)
    lazy val choiceName: LfChoiceName = LfChoiceName.assertFromString(raw.partial.choice)

    // Convert Daml-LF values to ledger API values.
    // In verbose mode, this involves loading Daml-LF packages and filling in missing type information.
    for {
      choiceArgument <- toApiValue(
        value = exerciseArgument,
        verbose = verbose,
        attribute = "exercise argument",
        enrich = value =>
          enricher.enrichChoiceArgument(templateId, interfaceId, choiceName, value.unversioned),
      )
      exerciseResult <- exerciseResult match {
        case Some(result) =>
          toApiValue(
            value = result,
            verbose = verbose,
            attribute = "exercise result",
            enrich = value =>
              enricher.enrichChoiceResult(templateId, interfaceId, choiceName, value.unversioned),
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

  def deserializeRaw(
      createdEvent: RawCreatedEvent,
      createArgument: Array[Byte],
      createArgumentCompression: Compression.Algorithm,
      createKeyValue: Option[Array[Byte]],
      createKeyValueCompression: Compression.Algorithm,
      templateId: LfIdentifier,
      witnesses: Set[String],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[ApiContractData] = {
    def getFatContractInstance(
        createArgument: VersionedValue,
        createKey: Option[VersionedValue],
    ): Option[Either[String, FatContractInstance]] =
      Option(createdEvent.driverMetadata).filter(_.nonEmpty).map { driverMetadataBytes =>
        for {
          contractId <- ContractId.fromString(createdEvent.contractId)
          signatories <- createdEvent.signatories.toList.traverse(Party.fromString).map(_.toSet)
          observers <- createdEvent.observers.toList.traverse(Party.fromString).map(_.toSet)
          maintainers <- createdEvent.createKeyMaintainers.toList
            .traverse(Party.fromString)
            .map(_.toSet)
          globalKey <- createKey
            .traverse(key =>
              GlobalKey
                .build(templateId, key.unversioned, Util.sharedKey(key.version))
                .left
                .map(_.msg)
            )
        } yield FatContractInstance.fromCreateNode(
          Node.Create(
            coid = contractId,
            templateId = createdEvent.templateId,
            arg = createArgument.unversioned,
            agreementText = createdEvent.agreementText.getOrElse(""),
            signatories = signatories,
            stakeholders = signatories ++ observers,
            keyOpt = globalKey.map(GlobalKeyWithMaintainers(_, maintainers)),
            version = createArgument.version,
          ),
          createTime = createdEvent.ledgerEffectiveTime,
          cantonData = Bytes.fromByteArray(driverMetadataBytes),
        )
      }

    for {
      createKey <- Future(
        createKeyValue.map(decompressAndDeserialize(createKeyValueCompression, _))
      )
      createArgument <- Future(
        decompressAndDeserialize(createArgumentCompression, createArgument)
      )
      apiContractData <- toApiContractData(
        value = createArgument,
        key = createKey,
        templateId = templateId,
        witnesses = witnesses,
        eventProjectionProperties = eventProjectionProperties,
        fatContractInstance = getFatContractInstance(createArgument, createKey),
      )
    } yield apiContractData
  }

  def toApiContractData(
      value: LfValue,
      key: Option[VersionedValue],
      templateId: LfIdentifier,
      witnesses: Set[String],
      eventProjectionProperties: EventProjectionProperties,
      fatContractInstance: => Option[Either[String, FatContractInstance]],
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[ApiContractData] = {

    val renderResult =
      eventProjectionProperties.render(witnesses, templateId)

    val verbose = eventProjectionProperties.verbose

    val asyncContractArguments = condFuture(renderResult.contractArguments)(
      enrichAsync(verbose, value.unversioned, enricher.enrichContract(templateId, _))
        .map(toContractArgumentApi(verbose))
    )
    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    val asyncContractKey = condFuture(renderResult.contractArguments && key.isDefined)(
      enrichAsync(verbose, key.get.unversioned, enricher.enrichContractKey(templateId, _))
        .map(toContractKeyApi(verbose))
    )
    val asyncInterfaceViews = Future.traverse(renderResult.interfaces.toList)(interfaceId =>
      computeInterfaceView(
        templateId,
        value.unversioned,
        interfaceId,
      ).flatMap(toInterfaceView(eventProjectionProperties.verbose, interfaceId))
    )

    val asyncCreatedEventBlob = condFuture(renderResult.createdEventBlob) {
      fatContractInstance
        .map { fatContractInstanceE =>
          (for {
            fatInstance <- fatContractInstanceE
            encoded <- TransactionCoder
              .encodeFatContractInstance(fatInstance)
              .left
              .map(_.errorMessage)
          } yield encoded).fold(
            err => Future.failed(new RuntimeException(s"Cannot serialize createdEventBlob: $err")),
            Future.successful,
          )
        }
        .getOrElse(Future.successful(ByteString.EMPTY))
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
      value: com.daml.lf.value.Value,
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
              .pipe(ErrorCause.DamlLf)
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

          case LfEngine.ResultInterruption(continue) =>
            goAsync(continue())

          case LfEngine.ResultNeedAuthority(_, _, _) =>
            Future.failed(new IllegalStateException("View computation must be a pure function"))

          case LfEngine.ResultNeedUpgradeVerification(_, _, _, _, _) =>
            Future.failed(new IllegalStateException("View computation must be a pure function"))

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
