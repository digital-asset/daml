// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.connection.v30
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc.ApiInfoServiceStub
import com.digitalasset.canton.connection.v30.GetApiInfoResponse
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Crypto, Fingerprint, SynchronizerCrypto}
import com.digitalasset.canton.health.{HealthElement, HealthListener}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasUnlessClosing, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{CommonMockMetrics, SequencerConnectionPoolMetrics}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.sequencer.api.v30 as SequencerService
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect
import com.digitalasset.canton.sequencer.api.v30.SequencerConnectServiceGrpc.SequencerConnectServiceStub
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencing.ConnectionX.{ConnectionXConfig, ConnectionXHealth}
import com.digitalasset.canton.sequencing.GrpcInternalSequencerConnectionX.GrpcSequencerConnectionXHealth
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.ConnectionAttributes
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.{
  SequencerConnectionXPoolConfig,
  SequencerConnectionXPoolHealth,
}
import com.digitalasset.canton.sequencing.SequencerSubscriptionPool.{
  SequencerSubscriptionPoolConfig,
  SequencerSubscriptionPoolHealth,
}
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth
import com.digitalasset.canton.sequencing.client.{
  SequencedEventValidator,
  SequencerClientSubscriptionError,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{
  Member,
  Namespace,
  ParticipantId,
  PhysicalSynchronizerId,
  SequencerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.{PekkoUtil, ResourceUtil}
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerAlias}
import io.grpc.stub.StreamObserver
import io.grpc.{CallOptions, Channel, Status}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalatest.Assertion
import org.scalatest.Assertions.fail
import org.scalatest.matchers.should.Matchers

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise, blocking}
import scala.util.Random

trait ConnectionPoolTestHelpers {
  this: BaseTest & HasExecutionContext =>
  import ConnectionPoolTestHelpers.*

  private lazy val seedForRandomness: Long = {
    val seed = Random.nextLong()
    logger.debug(s"Seed for randomness = $seed")
    seed
  }

  protected lazy val authConfig: AuthenticationTokenManagerConfig =
    AuthenticationTokenManagerConfig()

  protected lazy val testCrypto: SynchronizerCrypto =
    SynchronizerCrypto(
      SymbolicCrypto
        .create(testedReleaseProtocolVersion, timeouts, loggerFactory),
      defaultStaticSynchronizerParameters,
    )

  private implicit val actorSystem: ActorSystem =
    PekkoUtil.createActorSystem(loggerFactory.threadName)

  private implicit val executionSequencerFactory: ExecutionSequencerFactory =
    PekkoUtil.createExecutionSequencerFactory(loggerFactory.threadName, noTracingLogger)

  override def afterAll(): Unit =
    LifeCycle.close(
      executionSequencerFactory,
      LifeCycle.toCloseableActorSystem(actorSystem, logger, timeouts),
    )(logger)

  protected lazy val testMember: Member = ParticipantId("test")

  protected def mkConnectionAttributes(
      synchronizerIndex: Int,
      sequencerIndex: Int,
  ): ConnectionAttributes =
    ConnectionAttributes(
      testSynchronizerId(synchronizerIndex),
      testSequencerId(sequencerIndex),
      defaultStaticSynchronizerParameters,
    )

  protected def mkDummyConnectionConfig(
      index: Int,
      endpointIndexO: Option[Int] = None,
      expectedSequencerIdO: Option[SequencerId] = None,
  ): ConnectionXConfig = {
    val endpoint = Endpoint(s"does-not-exist-${endpointIndexO.getOrElse(index)}", Port.tryCreate(0))
    ConnectionXConfig(
      name = s"test-$index",
      endpoint = endpoint,
      transportSecurity = false,
      customTrustCertificates = None,
      expectedSequencerIdO = expectedSequencerIdO,
      tracePropagation = TracingConfig.Propagation.Disabled,
    )
  }

  protected def withLowLevelConnection[V]()(
      f: (ConnectionX, TestHealthListener[ConnectionXHealth]) => V
  ): V = {
    val config = mkDummyConnectionConfig(0)

    val connection = GrpcConnectionX(
      config,
      CommonMockMetrics.sequencerClient.connectionPool,
      timeouts,
      loggerFactory,
    )

    val listener = new TestHealthListener(connection.health)
    connection.health.registerOnHealthChange(listener)

    ResourceUtil.withResource(connection)(f(_, listener))
  }

  protected def withConnection[V](
      testResponses: TestResponses,
      expectedSequencerIdO: Option[SequencerId] = None,
  )(
      f: (InternalSequencerConnectionX, TestHealthListener[GrpcSequencerConnectionXHealth]) => V
  ): V = {
    val stubFactory = new TestSequencerConnectionXStubFactory(testResponses, loggerFactory)
    val config = mkDummyConnectionConfig(0, expectedSequencerIdO = expectedSequencerIdO)

    val connection = new GrpcInternalSequencerConnectionX(
      config,
      clientProtocolVersions,
      minimumProtocolVersion,
      stubFactory,
      CommonMockMetrics.sequencerClient.connectionPool,
      MetricsContext.Empty,
      futureSupervisor,
      timeouts,
      loggerFactory.append("connection", config.name),
    )

    val listener = new TestHealthListener(connection.health)
    connection.health.registerOnHealthChange(listener)

    ResourceUtil.withResource(connection)(f(_, listener))
  }

  protected def mkPoolConfig(
      nbConnections: PositiveInt,
      trustThreshold: PositiveInt,
      expectedSynchronizerIdO: Option[PhysicalSynchronizerId] = None,
      poolDelays: SequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
  ): SequencerConnectionXPoolConfig = {
    val configs =
      NonEmpty.from((0 until nbConnections.unwrap).map(mkDummyConnectionConfig(_))).value

    SequencerConnectionXPoolConfig(
      connections = configs,
      trustThreshold = trustThreshold,
      minRestartConnectionDelay = poolDelays.minRestartDelay,
      maxRestartConnectionDelay = poolDelays.maxRestartDelay,
      warnConnectionValidationDelay = poolDelays.warnValidationDelay,
      expectedPSIdO = expectedSynchronizerIdO,
    )
  }

  protected def withConnectionPool[V](
      nbConnections: PositiveInt,
      trustThreshold: PositiveInt,
      attributesForConnection: Int => ConnectionAttributes,
      responsesForConnection: PartialFunction[Int, TestResponses] = Map(),
      expectedSynchronizerIdO: Option[PhysicalSynchronizerId] = None,
      testTimeouts: ProcessingTimeout = timeouts,
      poolDelays: SequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
      blockValidation: Int => Boolean = _ => false,
  )(
      f: (
          SequencerConnectionXPool,
          CreatedConnections,
          TestHealthListener[SequencerConnectionXPoolHealth],
          Int => Unit,
      ) => V
  ): V = {
    val config = mkPoolConfig(
      nbConnections,
      trustThreshold,
      expectedSynchronizerIdO,
      poolDelays,
    )

    val validationBlocker = new TestValidationBlocker(blockValidation)

    val poolFactory = new TestSequencerConnectionXPoolFactory(
      attributesForConnection,
      responsesForConnection,
      validationBlocker,
      authConfig,
      testMember,
      wallClock,
      testCrypto.crypto,
      Some(seedForRandomness),
      metrics = CommonMockMetrics.sequencerClient.connectionPool,
      futureSupervisor,
      testTimeouts,
      loggerFactory,
    )
    val pool = poolFactory.create(config, name = "test").valueOrFail("create connection pool")

    val listener = new TestHealthListener(pool.health)
    pool.health.registerOnHealthChange(listener)

    ResourceUtil.withResource(validationBlocker)(blocker =>
      ResourceUtil.withResource(pool)(
        f(_, poolFactory.createdConnections, listener, blocker.unblock)
      )
    )
  }

  protected def mkSubscriptionPoolConfig(
      livenessMargin: NonNegativeInt,
      poolDelays: SequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
  ): SequencerSubscriptionPoolConfig =
    SequencerSubscriptionPoolConfig(
      livenessMargin = livenessMargin,
      subscriptionRequestDelay = poolDelays.subscriptionRequestDelay,
    )

  protected def withSubscriptionPool[V](
      livenessMargin: NonNegativeInt,
      connectionPool: SequencerConnectionXPool,
  )(f: (SequencerSubscriptionPool, TestHealthListener[SequencerSubscriptionPoolHealth]) => V): V = {
    val config = mkSubscriptionPoolConfig(livenessMargin)

    val subscriptionPoolFactory = new SequencerSubscriptionPoolFactoryImpl(
      sequencerSubscriptionFactory = new TestSequencerSubscriptionXFactory(timeouts, loggerFactory),
      subscriptionHandlerFactory = TestSubscriptionHandlerXFactory,
      metrics = CommonMockMetrics.sequencerClient.connectionPool,
      metricsContext = MetricsContext.Empty,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )
    val subscriptionPool = subscriptionPoolFactory.create(
      initialConfig = config,
      connectionPool = connectionPool,
      member = testMember,
      initialSubscriptionEventO = None,
      mock[SequencerAggregator],
    )

    val listener = new TestHealthListener(subscriptionPool.health)
    subscriptionPool.health.registerOnHealthChange(listener)

    ResourceUtil.withResource(subscriptionPool)(f(_, listener))
  }

  protected def withConnectionAndSubscriptionPools[V](
      nbConnections: PositiveInt,
      trustThreshold: PositiveInt,
      attributesForConnection: Int => ConnectionAttributes,
      responsesForConnection: PartialFunction[Int, TestResponses] = Map(),
      expectedSynchronizerIdO: Option[PhysicalSynchronizerId] = None,
      livenessMargin: NonNegativeInt,
  )(f: (SequencerSubscriptionPool, TestHealthListener[SequencerSubscriptionPoolHealth]) => V): V =
    withConnectionPool(
      nbConnections,
      trustThreshold,
      attributesForConnection,
      responsesForConnection,
      expectedSynchronizerIdO,
    ) { (connectionPool, _, _, _) =>
      connectionPool.start().futureValueUS.valueOrFail("initialization")

      withSubscriptionPool(livenessMargin, connectionPool) {
        (subscriptionPool, subscriptionPoolListener) =>
          f(subscriptionPool, subscriptionPoolListener)
      }
    }

}

protected object ConnectionPoolTestHelpers {
  import BaseTest.*

  lazy val failureUnavailable: Either[Exception, Nothing] =
    Left(Status.UNAVAILABLE.asRuntimeException())

  lazy val correctApiResponse: Either[Exception, GetApiInfoResponse] =
    Right(v30.GetApiInfoResponse(CantonGrpcUtil.ApiName.SequencerPublicApi))
  lazy val incorrectApiResponse: Either[Exception, GetApiInfoResponse] =
    Right(v30.GetApiInfoResponse("this is not a valid API info"))

  lazy val successfulHandshake: Either[Exception, SequencerConnect.HandshakeResponse] =
    Right(
      SequencerConnect.HandshakeResponse(
        testedProtocolVersion.toProtoPrimitive,
        SequencerConnect.HandshakeResponse.Value
          .Success(SequencerConnect.HandshakeResponse.Success()),
      )
    )
  lazy val failedHandshake: Either[Exception, SequencerConnect.HandshakeResponse] = Right(
    SequencerConnect.HandshakeResponse(
      testedProtocolVersion.toProtoPrimitive,
      SequencerConnect.HandshakeResponse.Value
        .Failure(SequencerConnect.HandshakeResponse.Failure("bad handshake")),
    )
  )

  lazy val correctSynchronizerIdResponse1
      : Either[Exception, SequencerConnect.GetSynchronizerIdResponse] = Right(
    SequencerConnect.GetSynchronizerIdResponse(
      testSynchronizerId(1).toProtoPrimitive,
      testSequencerId(1).uid.toProtoPrimitive,
    )
  )
  lazy val correctSynchronizerIdResponse2
      : Either[Exception, SequencerConnect.GetSynchronizerIdResponse] = Right(
    SequencerConnect.GetSynchronizerIdResponse(
      testSynchronizerId(2).toProtoPrimitive,
      testSequencerId(2).uid.toProtoPrimitive,
    )
  )

  lazy val correctStaticParametersResponse
      : Either[Exception, SequencerConnect.GetSynchronizerParametersResponse] = Right(
    SequencerConnect.GetSynchronizerParametersResponse(
      SequencerConnect.GetSynchronizerParametersResponse.Parameters.ParametersV1(
        defaultStaticSynchronizerParameters.toProtoV30
      )
    )
  )

  lazy val positiveAcknowledgeResponse
      : Either[Exception, SequencerService.AcknowledgeSignedResponse] = Right(
    SequencerService.AcknowledgeSignedResponse()
  )

  lazy val correctConnectionAttributes: ConnectionAttributes = ConnectionAttributes(
    testSynchronizerId(1),
    testSequencerId(1),
    defaultStaticSynchronizerParameters,
  )

  private lazy val clientProtocolVersions: NonEmpty[List[ProtocolVersion]] =
    ProtocolVersionCompatibility.supportedProtocols(
      includeAlphaVersions = true,
      includeBetaVersions = true,
      release = ReleaseVersion.current,
    )

  private lazy val minimumProtocolVersion: Option[ProtocolVersion] = Some(testedProtocolVersion)

  def testSynchronizerId(index: Int): PhysicalSynchronizerId =
    SynchronizerId.tryFromString(s"test-synchronizer-$index::namespace").toPhysical

  def testSequencerId(index: Int): SequencerId =
    SequencerId.tryCreate(
      s"test-sequencer-$index",
      Namespace(Fingerprint.tryFromString("namespace")),
    )

  private class TestSequencerSubscriptionXFactory(
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ) extends SequencerSubscriptionXFactory {
    override def create(
        connection: SequencerConnectionX,
        member: Member,
        preSubscriptionEventO: Option[ProcessingSerializedEvent],
        subscriptionHandlerFactory: SubscriptionHandlerXFactory,
        parent: HasUnlessClosing,
    )(implicit
        traceContext: TraceContext,
        ec: ExecutionContext,
    ): SequencerSubscriptionX[SequencerClientSubscriptionError] =
      new SequencerSubscriptionX(
        connection = connection,
        member = member,
        startingTimestampO = None,
        handler = _ => FutureUnlessShutdown.pure(Right(())),
        parent = parent,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
      )
  }

  private object TestSubscriptionHandlerXFactory extends SubscriptionHandlerXFactory {
    override def create(
        eventValidator: SequencedEventValidator,
        initialPriorEvent: Option[ProcessingSerializedEvent],
        sequencerAlias: SequencerAlias,
        sequencerId: SequencerId,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext): SubscriptionHandlerX = ???
  }

  private class TestSequencerConnectionXPoolFactory(
      attributesForConnection: Int => ConnectionAttributes,
      responsesForConnection: PartialFunction[Int, TestResponses],
      validationBlocker: TestValidationBlocker,
      authConfig: AuthenticationTokenManagerConfig,
      member: Member,
      clock: Clock,
      crypto: Crypto,
      seedForRandomnessO: Option[Long],
      metrics: SequencerConnectionPoolMetrics,
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ) extends SequencerConnectionXPoolFactory {

    import SequencerConnectionXPool.{SequencerConnectionXPoolConfig, SequencerConnectionXPoolError}

    private val connectionFactory = new TestInternalSequencerConnectionXFactory(
      attributesForConnection,
      responsesForConnection,
      validationBlocker,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )

    val createdConnections: CreatedConnections = connectionFactory.createdConnections

    override def create(
        initialConfig: SequencerConnectionXPoolConfig,
        name: String,
    )(implicit
        ec: ExecutionContextExecutor,
        esf: ExecutionSequencerFactory,
        materializer: Materializer,
    ): Either[SequencerConnectionXPoolError, SequencerConnectionXPool] =
      for {
        _ <- initialConfig.validate
      } yield {
        new SequencerConnectionXPoolImpl(
          initialConfig,
          connectionFactory,
          clock,
          authConfig,
          member,
          crypto,
          seedForRandomnessO,
          metrics,
          MetricsContext.Empty,
          futureSupervisor,
          timeouts,
          loggerFactory,
        )
      }

    override def createFromOldConfig(
        sequencerConnections: SequencerConnections,
        expectedPSIdO: Option[PhysicalSynchronizerId],
        tracingConfig: TracingConfig,
        name: String,
    )(implicit
        ec: ExecutionContextExecutor,
        esf: ExecutionSequencerFactory,
        materializer: Materializer,
        traceContext: TraceContext,
    ): Either[SequencerConnectionXPoolError, SequencerConnectionXPool] = ???
  }

  protected class TestInternalSequencerConnectionXFactory(
      attributesForConnection: Int => ConnectionAttributes,
      responsesForConnection: PartialFunction[Int, TestResponses],
      validationBlocker: TestValidationBlocker,
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ) extends InternalSequencerConnectionXFactory {
    val createdConnections = new CreatedConnections

    override def create(config: ConnectionXConfig)(implicit
        ec: ExecutionContextExecutor,
        esf: ExecutionSequencerFactory,
        materializer: Materializer,
    ): InternalSequencerConnectionX = {
      val s"test-$indexStr" = config.name: @unchecked
      val index = indexStr.toInt

      val attributes = attributesForConnection(index)
      val correctSynchronizerIdResponse = Right(
        SequencerConnect.GetSynchronizerIdResponse(
          attributes.physicalSynchronizerId.toProtoPrimitive,
          attributes.sequencerId.uid.toProtoPrimitive,
        )
      )

      val responses = responsesForConnection.applyOrElse(
        index,
        (_: Int) =>
          new TestResponses(
            apiResponses = Iterator.continually(correctApiResponse),
            handshakeResponses = Iterator.continually(successfulHandshake),
            synchronizerAndSeqIdResponses = Iterator.continually(correctSynchronizerIdResponse),
            staticParametersResponses = Iterator.continually(correctStaticParametersResponse),
            acknowledgeResponses = Iterator.continually(positiveAcknowledgeResponse),
            validationBlocker.delayF(index),
          ),
      )

      val stubFactory = new TestSequencerConnectionXStubFactory(responses, loggerFactory)

      val connection = new GrpcInternalSequencerConnectionX(
        config,
        clientProtocolVersions,
        minimumProtocolVersion,
        stubFactory,
        CommonMockMetrics.sequencerClient.connectionPool,
        MetricsContext.Empty,
        futureSupervisor,
        timeouts,
        loggerFactory.append("connection", config.name),
      )

      createdConnections.add(index, connection)

      connection
    }
  }

  protected class CreatedConnections {
    private val connectionsMap = TrieMap[Int, InternalSequencerConnectionX]()

    def apply(index: Int): InternalSequencerConnectionX = connectionsMap.apply(index)

    def add(index: Int, connection: InternalSequencerConnectionX): Unit =
      blocking {
        synchronized {
          connectionsMap.updateWith(index) {
            case Some(_) => throw new IllegalStateException("Connection already exists")
            case None => Some(connection)
          }
        }
      }

    def snapshotAndClear(): Map[Int, InternalSequencerConnectionX] = blocking {
      synchronized {
        val snapshot = connectionsMap.readOnlySnapshot().toMap
        connectionsMap.clear()
        snapshot
      }
    }

    def size: Int = connectionsMap.size
  }

  protected class TestResponses(
      apiResponses: Iterator[Either[Exception, v30.GetApiInfoResponse]] = Iterator.empty,
      handshakeResponses: Iterator[Either[Exception, SequencerConnect.HandshakeResponse]] =
        Iterator.empty,
      synchronizerAndSeqIdResponses: Iterator[
        Either[Exception, SequencerConnect.GetSynchronizerIdResponse]
      ] = Iterator.empty,
      staticParametersResponses: Iterator[
        Either[Exception, SequencerConnect.GetSynchronizerParametersResponse]
      ] = Iterator.empty,
      acknowledgeResponses: Iterator[
        Either[Exception, SequencerService.AcknowledgeSignedResponse]
      ] = Iterator.empty,
      delayF: Future[Unit] = Future.unit,
  )(implicit ec: ExecutionContext)
      extends Matchers {
    private class TestApiInfoServiceStub(
        channel: Channel,
        options: CallOptions = CallOptions.DEFAULT,
    ) extends ApiInfoServiceStub(channel, options) {
      override def getApiInfo(request: v30.GetApiInfoRequest): Future[v30.GetApiInfoResponse] = {
        withClue("call is not authenticated") {
          options.getCredentials shouldBe null
        }
        nextResponse(apiResponses)
      }

      override def build(channel: Channel, options: CallOptions): ApiInfoServiceStub =
        new TestApiInfoServiceStub(channel, options)
    }

    private class TestSequencerConnectServiceStub(
        channel: Channel,
        options: CallOptions = CallOptions.DEFAULT,
    ) extends SequencerConnectServiceStub(channel, options) {
      override def handshake(
          request: SequencerConnect.HandshakeRequest
      ): Future[SequencerConnect.HandshakeResponse] =
        nextResponse(handshakeResponses)

      override def getSynchronizerId(
          request: SequencerConnect.GetSynchronizerIdRequest
      ): Future[SequencerConnect.GetSynchronizerIdResponse] =
        nextResponse(synchronizerAndSeqIdResponses)

      override def getSynchronizerParameters(
          request: SequencerConnect.GetSynchronizerParametersRequest
      ): Future[SequencerConnect.GetSynchronizerParametersResponse] =
        nextResponse(staticParametersResponses)

      override def build(channel: Channel, options: CallOptions): SequencerConnectServiceStub =
        new TestSequencerConnectServiceStub(channel, options)
    }

    private class TestSequencerServiceStub(
        channel: Channel,
        options: CallOptions = CallOptions.DEFAULT,
    ) extends SequencerServiceStub(channel, options) {
      override def sendAsync(
          request: SequencerService.SendAsyncRequest
      ): Future[SequencerService.SendAsyncResponse] = ???

      override def subscribe(
          request: SequencerService.SubscriptionRequest,
          responseObserver: StreamObserver[SequencerService.SubscriptionResponse],
      ): Unit = ()

      override def acknowledgeSigned(
          request: SequencerService.AcknowledgeSignedRequest
      ): scala.concurrent.Future[SequencerService.AcknowledgeSignedResponse] = {
        withClue("call is authenticated") {
          Option(options.getCredentials) shouldBe defined
        }
        nextResponse(acknowledgeResponses)
      }

      override def downloadTopologyStateForInit(
          request: SequencerService.DownloadTopologyStateForInitRequest,
          responseObserver: StreamObserver[
            SequencerService.DownloadTopologyStateForInitResponse
          ],
      ): Unit = ???

      override def getTrafficStateForMember(
          request: com.digitalasset.canton.sequencer.api.v30.GetTrafficStateForMemberRequest
      ): scala.concurrent.Future[
        com.digitalasset.canton.sequencer.api.v30.GetTrafficStateForMemberResponse
      ] = ???

      override def build(channel: Channel, options: CallOptions): SequencerServiceStub =
        new TestSequencerServiceStub(channel, options)
    }

    def apiSvcFactory(channel: Channel): ApiInfoServiceStub =
      new TestApiInfoServiceStub(channel)

    def sequencerConnectSvcFactory(channel: Channel): SequencerConnectServiceStub =
      new TestSequencerConnectServiceStub(channel)

    def sequencerSvcFactory(channel: Channel): SequencerServiceStub =
      new TestSequencerServiceStub(channel)

    private def nextResponse[T](responses: Iterator[Either[Exception, T]]): Future[T] = {
      val f: Future[T] =
        if (responses.hasNext) responses.next().fold(Future.failed, Future.successful)
        else Future.failed(Status.UNAVAILABLE.asRuntimeException())

      delayF.flatMap(_ => f)
    }

    def assertAllResponsesSent(): Assertion = {
      withClue("API responses:")(apiResponses shouldBe empty)
      withClue("Handshake responses:")(handshakeResponses shouldBe empty)
      withClue("Synchronizer and sequencer ID responses:")(
        synchronizerAndSeqIdResponses shouldBe empty
      )
      withClue("Static synchronizer parameters responses:")(
        staticParametersResponses shouldBe empty
      )
      withClue("Acknowledge responses:")(
        acknowledgeResponses shouldBe empty
      )
    }
  }

  object TestResponses {
    def apply(
        apiResponses: Seq[Either[Exception, v30.GetApiInfoResponse]] = Seq.empty,
        handshakeResponses: Seq[Either[Exception, SequencerConnect.HandshakeResponse]] = Seq.empty,
        synchronizerAndSeqIdResponses: Seq[
          Either[Exception, SequencerConnect.GetSynchronizerIdResponse]
        ] = Seq.empty,
        staticParametersResponses: Seq[
          Either[Exception, SequencerConnect.GetSynchronizerParametersResponse]
        ] = Seq.empty,
        acknowledgeResponses: Seq[
          Either[Exception, SequencerService.AcknowledgeSignedResponse]
        ] = Seq.empty,
        delayF: Future[Unit] = Future.unit,
    )(implicit ec: ExecutionContext): TestResponses = new TestResponses(
      apiResponses.iterator,
      handshakeResponses.iterator,
      synchronizerAndSeqIdResponses.iterator,
      staticParametersResponses.iterator,
      acknowledgeResponses.iterator,
      delayF,
    )
  }

  protected class TestSequencerConnectionXStubFactory(
      testResponses: TestResponses,
      loggerFactory: NamedLoggerFactory,
  ) extends SequencerConnectionXStubFactory {
    override def createStub(connection: ConnectionX, metricsContext: MetricsContext)(implicit
        ec: ExecutionContextExecutor
    ): SequencerConnectionXStub = connection match {
      case grpcConnection: GrpcConnectionX =>
        new GrpcSequencerConnectionXStub(
          grpcConnection,
          testResponses.apiSvcFactory,
          testResponses.sequencerConnectSvcFactory,
          metricsContext,
        )

      case _ => throw new IllegalStateException(s"Connection type not supported: $connection")
    }

    override def createUserStub(
        connection: ConnectionX,
        clientAuth: GrpcSequencerClientAuth,
        metricsContext: MetricsContext,
        timeouts: ProcessingTimeout,
        protocolVersion: ProtocolVersion,
    )(implicit
        ec: ExecutionContextExecutor,
        esf: ExecutionSequencerFactory,
        materializer: Materializer,
    ): UserSequencerConnectionXStub =
      connection match {
        case grpcConnection: GrpcConnectionX =>
          new GrpcUserSequencerConnectionXStub(
            grpcConnection,
            channel => clientAuth(testResponses.sequencerSvcFactory(channel)),
            metricsContext,
            timeouts,
            loggerFactory,
            protocolVersion,
          )

        case _ => throw new IllegalStateException(s"Connection type not supported: $connection")
      }
  }

  private class TestHealthListener[HE <: HealthElement](val element: HE)
      extends HealthListener
      with Matchers {
    import scala.collection.mutable
    import BaseTest.eventuallyForever

    private val statesBuffer = mutable.ArrayBuffer[element.State]()

    def shouldStabilizeOn(state: element.State): Assertion =
      // Check that we reach the given state, and remain on it
      // The default 2 seconds is a bit short when machines are under heavy load
      eventuallyForever(timeUntilSuccess = 10.seconds) {
        statesBuffer.last shouldBe state
      }

    def clear(): Unit = statesBuffer.clear()

    override def name: String = s"${element.name}-test-listener"

    override def poke()(implicit traceContext: TraceContext): Unit = blocking {
      synchronized {
        val state = element.getState

        statesBuffer += state
      }
    }
  }

  private class TestValidationBlocker(private val blockValidation: Int => Boolean)
      extends AutoCloseable {
    private val promises = TrieMap[Int, Promise[Unit]]()

    def delayF(index: Int): Future[Unit] =
      if (blockValidation(index)) {
        val p = Promise[Unit]()
        promises.put(index, p).foreach(_ => fail(s"Connection #$index was already blocked"))
        p.future
      } else Future.unit

    def unblock(index: Int): Unit =
      promises.getOrElse(index, fail(s"Connection #$index was not blocked")).trySuccess(())

    override def close(): Unit = promises.values.foreach(_.trySuccess(()))
  }
}
