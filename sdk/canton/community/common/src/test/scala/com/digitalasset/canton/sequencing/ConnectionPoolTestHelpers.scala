// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.connection.v30
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc.ApiInfoServiceStub
import com.digitalasset.canton.connection.v30.GetApiInfoResponse
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect
import com.digitalasset.canton.sequencer.api.v30.SequencerConnectServiceGrpc.SequencerConnectServiceStub
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.sequencing.SequencerConnectionX.ConnectionAttributes
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.SequencerConnectionXPoolConfig
import com.digitalasset.canton.topology.{Namespace, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.{CallOptions, Channel, Status}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContextExecutor, Future, blocking}
import scala.util.Random

trait ConnectionPoolTestHelpers { this: BaseTest & HasExecutionContext =>
  protected lazy val failureUnavailable: Either[Exception, Nothing] =
    Left(Status.UNAVAILABLE.asRuntimeException())

  protected lazy val correctApiResponse: Either[Exception, GetApiInfoResponse] =
    Right(v30.GetApiInfoResponse(CantonGrpcUtil.ApiName.SequencerPublicApi))
  protected lazy val incorrectApiResponse: Either[Exception, GetApiInfoResponse] =
    Right(v30.GetApiInfoResponse("this is not a valid API info"))

  protected lazy val successfulHandshake: Either[Exception, SequencerConnect.HandshakeResponse] =
    Right(
      SequencerConnect.HandshakeResponse(
        testedProtocolVersion.toProtoPrimitive,
        SequencerConnect.HandshakeResponse.Value
          .Success(SequencerConnect.HandshakeResponse.Success()),
      )
    )
  protected lazy val failedHandshake: Either[Exception, SequencerConnect.HandshakeResponse] = Right(
    SequencerConnect.HandshakeResponse(
      testedProtocolVersion.toProtoPrimitive,
      SequencerConnect.HandshakeResponse.Value
        .Failure(SequencerConnect.HandshakeResponse.Failure("bad handshake")),
    )
  )

  protected lazy val correctSynchronizerIdResponse1
      : Either[Exception, SequencerConnect.GetSynchronizerIdResponse] = Right(
    SequencerConnect.GetSynchronizerIdResponse(
      testSynchronizerId(1).toProtoPrimitive,
      testSequencerId(1).uid.toProtoPrimitive,
    )
  )
  protected lazy val correctSynchronizerIdResponse2
      : Either[Exception, SequencerConnect.GetSynchronizerIdResponse] = Right(
    SequencerConnect.GetSynchronizerIdResponse(
      testSynchronizerId(2).toProtoPrimitive,
      testSequencerId(2).uid.toProtoPrimitive,
    )
  )

  protected lazy val correctStaticParametersResponse
      : Either[Exception, SequencerConnect.GetSynchronizerParametersResponse] = Right(
    SequencerConnect.GetSynchronizerParametersResponse(
      SequencerConnect.GetSynchronizerParametersResponse.Parameters.ParametersV1(
        defaultStaticSynchronizerParameters.toProtoV30
      )
    )
  )

  protected lazy val correctConnectionAttributes: ConnectionAttributes = ConnectionAttributes(
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

  private lazy val seedForRandomness: Long = {
    val seed = Random.nextLong()
    logger.debug(s"Seed for randomness = $seed")
    seed
  }

  protected def testSynchronizerId(index: Int): SynchronizerId =
    SynchronizerId.tryFromString(s"test-synchronizer-$index::namespace")
  protected def testSequencerId(index: Int): SequencerId =
    SequencerId.tryCreate(
      s"test-sequencer-$index",
      Namespace(Fingerprint.tryFromString("namespace")),
    )

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
  ): ConnectionXConfig = {
    val endpoint = Endpoint(s"does-not-exist-${endpointIndexO.getOrElse(index)}", Port.tryCreate(0))
    ConnectionXConfig(
      name = s"test-$index",
      endpoint = endpoint,
      transportSecurity = false,
      customTrustCertificates = None,
      tracePropagation = TracingConfig.Propagation.Disabled,
    )
  }

  protected def withConnection[V](
      testResponses: TestResponses
  )(f: (SequencerConnectionX, TestHealthListener) => V): V = {
    val clientFactory = new TestSequencerConnectionXClientFactory(testResponses)
    val config = mkDummyConnectionConfig(0)

    val connection = new GrpcSequencerConnectionX(
      config,
      clientProtocolVersions,
      minimumProtocolVersion,
      clientFactory,
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
      expectedSynchronizerIdO: Option[SynchronizerId] = None,
  ): SequencerConnectionXPoolConfig = {
    val configs =
      NonEmpty.from((0 until nbConnections.unwrap).map(mkDummyConnectionConfig(_))).value

    SequencerConnectionXPoolConfig(
      connections = configs,
      trustThreshold = trustThreshold,
      expectedSynchronizerIdO = expectedSynchronizerIdO,
    )
  }

  protected def withConnectionPool[V](
      nbConnections: PositiveInt,
      trustThreshold: PositiveInt,
      attributesForConnection: Int => ConnectionAttributes,
      expectedSynchronizerIdO: Option[SynchronizerId] = None,
  )(f: (SequencerConnectionXPoolImpl, CreatedConnections, TestHealthListener) => V): V = {
    val config = mkPoolConfig(nbConnections, trustThreshold, expectedSynchronizerIdO)

    val connectionFactory = new TestSequencerConnectionXFactory(attributesForConnection)
    val pool =
      SequencerConnectionXPoolFactory
        .create(
          config,
          connectionFactory,
          wallClock,
          seedForRandomnessO = Some(seedForRandomness),
          timeouts,
          loggerFactory,
        )
        .valueOrFail("create connection pool")

    val listener = new TestHealthListener(pool.health)
    pool.health.registerOnHealthChange(listener)

    ResourceUtil.withResource(pool)(f(_, connectionFactory.createdConnections, listener))
  }

  protected class TestSequencerConnectionXFactory(
      attributesForConnection: Int => ConnectionAttributes
  ) extends SequencerConnectionXFactory {
    val createdConnections = new CreatedConnections

    override def create(config: ConnectionXConfig)(implicit
        ec: ExecutionContextExecutor
    ): SequencerConnectionX = {
      val s"test-$indexStr" = config.name: @unchecked
      val index = indexStr.toInt

      val attributes = attributesForConnection(index)
      val correctSynchronizerIdResponse = Right(
        SequencerConnect.GetSynchronizerIdResponse(
          attributes.synchronizerId.toProtoPrimitive,
          attributes.sequencerId.uid.toProtoPrimitive,
        )
      )

      val responses = new TestResponses(
        apiResponses = Iterator.continually(correctApiResponse),
        handshakeResponses = Iterator.continually(successfulHandshake),
        synchronizerAndSeqIdResponses = Iterator.continually(correctSynchronizerIdResponse),
        staticParametersResponses = Iterator.continually(correctStaticParametersResponse),
      )

      val clientFactory = new TestSequencerConnectionXClientFactory(responses)

      val connection = new GrpcSequencerConnectionX(
        config,
        clientProtocolVersions,
        minimumProtocolVersion,
        clientFactory,
        futureSupervisor,
        timeouts,
        loggerFactory.append("connection", config.name),
      )(ec)

      createdConnections.add(index, connection)

      connection
    }
  }

  protected class CreatedConnections {
    private val connectionsMap = TrieMap[Int, SequencerConnectionX]()

    def apply(index: Int): SequencerConnectionX = connectionsMap.apply(index)

    def add(index: Int, connection: SequencerConnectionX): Unit =
      blocking {
        synchronized {
          connectionsMap.updateWith(index) {
            case Some(_) => throw new IllegalStateException("Connection already exists")
            case None => Some(connection)
          }
        }
      }

    def snapshotAndClear(): Map[Int, SequencerConnectionX] = blocking {
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
  ) extends Matchers {
    val apiSvcFactory: Channel => ApiInfoServiceStub = channel =>
      new ApiInfoServiceStub(channel) {
        override def getApiInfo(request: v30.GetApiInfoRequest): Future[v30.GetApiInfoResponse] =
          nextResponse(apiResponses)

        override def build(channel: Channel, options: CallOptions): ApiInfoServiceStub = this
      }

    val sequencerConnectSvcFactory: Channel => SequencerConnectServiceStub =
      channel =>
        new SequencerConnectServiceStub(channel) {
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
            this
        }

    private def nextResponse[T](responses: Iterator[Either[Exception, T]]): Future[T] =
      if (responses.hasNext) responses.next().fold(Future.failed, Future.successful)
      else Future.failed(Status.UNAVAILABLE.asRuntimeException())

    def assertAllResponsesSent(): Assertion = {
      withClue("API responses:")(apiResponses shouldBe empty)
      withClue("Handshake responses:")(handshakeResponses shouldBe empty)
      withClue("Synchronizer and sequencer ID responses:")(
        synchronizerAndSeqIdResponses shouldBe empty
      )
      withClue("Static synchronizer parameters responses:")(
        staticParametersResponses shouldBe empty
      )
    }
  }

  protected object TestResponses {
    def apply(
        apiResponses: Seq[Either[Exception, v30.GetApiInfoResponse]] = Seq.empty,
        handshakeResponses: Seq[Either[Exception, SequencerConnect.HandshakeResponse]] = Seq.empty,
        synchronizerAndSeqIdResponses: Seq[
          Either[Exception, SequencerConnect.GetSynchronizerIdResponse]
        ] = Seq.empty,
        staticParametersResponses: Seq[
          Either[Exception, SequencerConnect.GetSynchronizerParametersResponse]
        ] = Seq.empty,
    ): TestResponses = new TestResponses(
      apiResponses.iterator,
      handshakeResponses.iterator,
      synchronizerAndSeqIdResponses.iterator,
      staticParametersResponses.iterator,
    )
  }

  protected class TestSequencerConnectionXClientFactory(testResponses: TestResponses)
      extends SequencerConnectionXClientFactory {
    override def create(connection: ConnectionX)(implicit
        ec: ExecutionContextExecutor
    ): SequencerConnectionXClient = connection match {
      case grpcConnection: GrpcConnectionX =>
        new GrpcSequencerConnectionXClient(
          grpcConnection,
          testResponses.apiSvcFactory,
          testResponses.sequencerConnectSvcFactory,
        )

      case _ => throw new IllegalStateException(s"Connection type not supported: $connection")
    }
  }
}
