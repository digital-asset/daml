// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import better.files.File
import cats.data.EitherT
import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{LocalNodeConfig, ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.GrpcVaultServiceFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPublicStoreError}
import com.digitalasset.canton.environment.CantonNodeBootstrap.HealthDumpFunction
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.health.admin.grpc.GrpcStatusService
import com.digitalasset.canton.health.admin.v0.StatusServiceGrpc
import com.digitalasset.canton.health.{
  DependenciesHealthService,
  GrpcHealthReporter,
  GrpcHealthServer,
  HealthService,
  LivenessHealthService,
  ServiceHealthStatusManager,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.metrics.MetricHandle.MetricsFactory
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonServerBuilder}
import com.digitalasset.canton.resource.{Storage, StorageFactory}
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, TracerProvider}
import com.digitalasset.canton.util.SimpleExecutionQueue
import io.grpc.protobuf.services.ProtoReflectionService
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

/** When a canton node is created it first has to obtain an identity before most of its services can be started.
  * This process will begin when `start` is called and will try to perform as much as permitted by configuration automatically.
  * If external action is required before this process can complete `start` will return successfully but `isInitialized` will still be false.
  * When the node is successfully initialized the underlying node will be available through `getNode`.
  */
trait CantonNodeBootstrap[+T <: CantonNode] extends FlagCloseable with NamedLogging {

  def name: InstanceName
  def clock: Clock
  def getId: Option[NodeId]
  def isInitialized: Boolean

  def start(): EitherT[Future, String, Unit]

  def getNode: Option[T]

  /** Access to the private and public store to support local key inspection commands */
  def crypto: Option[Crypto]
  def isActive: Boolean
}

object CantonNodeBootstrap {
  type HealthDumpFunction = () => Future[File]
}

trait BaseMetrics {
  def prefix: MetricName
  @nowarn("cat=deprecation")
  def metricsFactory: MetricsFactory
  def grpcMetrics: GrpcServerMetrics
  def healthMetrics: HealthMetrics
  def storageMetrics: DbStorageMetrics
}

final case class CantonNodeBootstrapCommonArguments[
    +NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    M <: BaseMetrics,
](
    name: InstanceName,
    config: NodeConfig,
    parameterConfig: ParameterConfig,
    testingConfig: TestingConfigInternal,
    clock: Clock,
    metrics: M,
    storageFactory: StorageFactory,
    cryptoFactory: CryptoFactory,
    cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
    grpcVaultServiceFactory: GrpcVaultServiceFactory,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
    writeHealthDumpToFile: HealthDumpFunction,
    configuredOpenTelemetry: ConfiguredOpenTelemetry,
    tracerProvider: TracerProvider,
)

abstract class CantonNodeBootstrapCommon[
    T <: CantonNode,
    NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    Metrics <: BaseMetrics,
](
    protected val arguments: CantonNodeBootstrapCommonArguments[
      NodeConfig,
      ParameterConfig,
      Metrics,
    ]
)(
    implicit val executionContext: ExecutionContextIdlenessExecutorService,
    implicit val scheduler: ScheduledExecutorService,
    implicit val actorSystem: ActorSystem,
) extends CantonNodeBootstrap[T]
    with HasCloseContext
    with NoTracing {

  override def name: InstanceName = arguments.name
  override def clock: Clock = arguments.clock
  def config: NodeConfig = arguments.config
  def parameterConfig: ParameterConfig = arguments.parameterConfig
  // TODO(#14048) unify parameters and parameterConfig
  def parameters: ParameterConfig = parameterConfig
  override def timeouts: ProcessingTimeout = arguments.parameterConfig.processingTimeouts
  override def loggerFactory: NamedLoggerFactory = arguments.loggerFactory
  protected def futureSupervisor: FutureSupervisor = arguments.futureSupervisor

  protected val cryptoConfig = config.crypto
  protected val adminApiConfig = config.adminApi
  protected val initConfig = config.init
  protected val tracerProvider = arguments.tracerProvider
  protected implicit val tracer: Tracer = tracerProvider.tracer
  protected val initQueue: SimpleExecutionQueue = new SimpleExecutionQueue(
    s"init-queue-${arguments.name}",
    arguments.futureSupervisor,
    timeouts,
    loggerFactory,
  )

  // This absolutely must be a "def", because it is used during class initialization.
  protected def connectionPoolForParticipant: Boolean = false

  protected val ips = new IdentityProvidingServiceClient()

  private def status: Future[NodeStatus[NodeStatus.Status]] = {
    getNode
      .map(_.status.map(NodeStatus.Success(_)))
      .getOrElse(Future.successful(NodeStatus.NotInitialized(isActive)))
  }

  protected def registerHealthGauge(): Unit = {
    arguments.metrics.healthMetrics
      .registerHealthGauge(
        name.toProtoPrimitive,
        () => getNode.map(_.status.map(_.active)).getOrElse(Future(false)),
      )
      .discard // we still want to report the health even if the node is closed
  }

  // The admin-API services
  logger.info(s"Starting admin-api services on ${adminApiConfig}")
  protected val (adminServer, adminServerRegistry) = {
    val builder = CantonServerBuilder
      .forConfig(
        adminApiConfig,
        arguments.metrics.prefix,
        arguments.metrics.metricsFactory,
        executionContext,
        loggerFactory,
        parameterConfig.loggingConfig.api,
        parameterConfig.tracing,
        arguments.metrics.grpcMetrics,
      )

    val registry = builder.mutableHandlerRegistry()

    val server = builder
      .addService(
        StatusServiceGrpc.bindService(
          new GrpcStatusService(
            status,
            arguments.writeHealthDumpToFile,
            parameterConfig.processingTimeouts,
            loggerFactory,
          ),
          executionContext,
        )
      )
      .addService(ProtoReflectionService.newInstance(), false)
      .addService(
        ApiInfoServiceGrpc.bindService(
          new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi),
          executionContext,
        )
      )
      .build
      .start()
    (Lifecycle.toCloseableServer(server, logger, "AdminServer"), registry)
  }

  protected def mkNodeHealthService(
      storage: Storage
  ): (DependenciesHealthService, LivenessHealthService)
  protected def mkHealthComponents(
      nodeHealthService: HealthService,
      nodeLivenessService: LivenessHealthService,
  ): (GrpcHealthReporter, Option[GrpcHealthServer]) = {
    val healthReporter: GrpcHealthReporter = new GrpcHealthReporter(loggerFactory)
    val grpcNodeHealthManager =
      ServiceHealthStatusManager(
        "Health API",
        new io.grpc.protobuf.services.HealthStatusManager(),
        Set(nodeHealthService, nodeLivenessService),
      )
    val grpcHealthServer = config.monitoring.grpcHealthServer.map { healthConfig =>
      healthReporter.registerHealthManager(grpcNodeHealthManager)

      val executor = Executors.newFixedThreadPool(healthConfig.parallelism)

      new GrpcHealthServer(
        healthConfig,
        arguments.metrics.metricsFactory,
        executor,
        loggerFactory,
        parameterConfig.loggingConfig.api,
        parameterConfig.tracing,
        arguments.metrics.grpcMetrics,
        timeouts,
        grpcNodeHealthManager.manager,
      )
    }
    (healthReporter, grpcHealthServer)
  }

}

object CantonNodeBootstrapCommon {

  def getOrCreateSigningKey(crypto: Crypto)(
      name: String
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, String, SigningPublicKey] =
    getOrCreateKey(
      "signing",
      crypto.cryptoPublicStore.findSigningKeyIdByName,
      name => crypto.generateSigningKey(name = name).leftMap(_.toString),
      crypto.cryptoPrivateStore.existsSigningKey,
      name,
    )

  def getOrCreateSigningKeyByFingerprint(crypto: Crypto)(
      fingerprint: Fingerprint
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, String, SigningPublicKey] =
    getKeyByFingerprint(
      "signing",
      crypto.cryptoPublicStore.findSigningKeyIdByFingerprint,
      crypto.cryptoPrivateStore.existsSigningKey,
      fingerprint,
    )

  def getOrCreateEncryptionKey(crypto: Crypto)(
      name: String
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, String, EncryptionPublicKey] =
    getOrCreateKey(
      "encryption",
      crypto.cryptoPublicStore.findEncryptionKeyIdByName,
      name => crypto.generateEncryptionKey(name = name).leftMap(_.toString),
      crypto.cryptoPrivateStore.existsDecryptionKey,
      name,
    )

  private def getKeyByFingerprint[P <: PublicKey](
      typ: String,
      findPubKeyIdByFingerprint: Fingerprint => EitherT[Future, CryptoPublicStoreError, Option[P]],
      existPrivateKeyByFp: Fingerprint => EitherT[Future, CryptoPrivateStoreError, Boolean],
      fingerprint: Fingerprint,
  )(implicit ec: ExecutionContext): EitherT[Future, String, P] = for {
    keyIdO <- findPubKeyIdByFingerprint(fingerprint)
      .leftMap(err =>
        s"Failure while looking for $typ fingerprint $fingerprint in public store: ${err}"
      )
    pubKey <- keyIdO.fold(
      EitherT.leftT[Future, P](s"$typ key with fingerprint $fingerprint does not exist")
    ) { keyWithFingerprint =>
      val fingerprint = keyWithFingerprint.fingerprint
      existPrivateKeyByFp(fingerprint)
        .leftMap(err =>
          s"Failure while looking for $typ key $fingerprint in private key store: $err"
        )
        .transform {
          case Right(true) => Right(keyWithFingerprint)
          case Right(false) =>
            Left(s"Broken private key store: Could not find $typ key $fingerprint")
          case Left(err) => Left(err)
        }
    }
  } yield pubKey

  private def getOrCreateKey[P <: PublicKey](
      typ: String,
      findPubKeyIdByName: KeyName => EitherT[Future, CryptoPublicStoreError, Option[P]],
      generateKey: Option[KeyName] => EitherT[Future, String, P],
      existPrivateKeyByFp: Fingerprint => EitherT[Future, CryptoPrivateStoreError, Boolean],
      name: String,
  )(implicit ec: ExecutionContext): EitherT[Future, String, P] = for {
    keyName <- EitherT.fromEither[Future](KeyName.create(name))
    keyIdO <- findPubKeyIdByName(keyName)
      .leftMap(err => s"Failure while looking for $typ key $name in public store: ${err}")
    pubKey <- keyIdO.fold(
      generateKey(Some(keyName))
        .leftMap(err => s"Failure while generating $typ key for $name: $err")
    ) { keyWithName =>
      val fingerprint = keyWithName.fingerprint
      existPrivateKeyByFp(fingerprint)
        .leftMap(err =>
          s"Failure while looking for $typ key $fingerprint of $name in private key store: $err"
        )
        .transform {
          case Right(true) => Right(keyWithName)
          case Right(false) =>
            Left(s"Broken private key store: Could not find $typ key $fingerprint of $name")
          case Left(err) => Left(err)
        }
    }
  } yield pubKey

}
