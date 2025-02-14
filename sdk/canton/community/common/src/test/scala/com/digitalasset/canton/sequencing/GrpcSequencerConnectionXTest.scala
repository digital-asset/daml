// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.connection.v30
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc.ApiInfoServiceStub
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect
import com.digitalasset.canton.sequencer.api.v30.SequencerConnectServiceGrpc.SequencerConnectServiceStub
import com.digitalasset.canton.sequencing.SequencerConnectionX.{
  ConnectionAttributes,
  SequencerConnectionXError,
  SequencerConnectionXState,
}
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.version.{ProtocolVersionCompatibility, ReleaseVersion}
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import io.grpc.{CallOptions, Channel, Status}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContextExecutor, Future}

class GrpcSequencerConnectionXTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ConnectionPoolTestHelpers {

  private lazy val failureUnavailable = Left(Status.UNAVAILABLE.asRuntimeException())

  private lazy val correctApiResponse =
    Right(v30.GetApiInfoResponse(CantonGrpcUtil.ApiName.SequencerPublicApi))
  private lazy val incorrectApiResponse =
    Right(v30.GetApiInfoResponse("this is not a valid API info"))

  private lazy val successfulHandshake = Right(
    SequencerConnect.HandshakeResponse(
      testedProtocolVersion.toProtoPrimitive,
      SequencerConnect.HandshakeResponse.Value
        .Success(SequencerConnect.HandshakeResponse.Success()),
    )
  )
  private lazy val failedHandshake = Right(
    SequencerConnect.HandshakeResponse(
      testedProtocolVersion.toProtoPrimitive,
      SequencerConnect.HandshakeResponse.Value
        .Failure(SequencerConnect.HandshakeResponse.Failure("bad handshake")),
    )
  )

  private lazy val correctSynchronizerIdResponse1 = Right(
    SequencerConnect.GetSynchronizerIdResponse(
      testSynchronizerId(1).toProtoPrimitive,
      testSequencerId(1).uid.toProtoPrimitive,
    )
  )
  private lazy val correctSynchronizerIdResponse2 = Right(
    SequencerConnect.GetSynchronizerIdResponse(
      testSynchronizerId(2).toProtoPrimitive,
      testSequencerId(2).uid.toProtoPrimitive,
    )
  )

  private lazy val correctStaticParametersResponse = Right(
    SequencerConnect.GetSynchronizerParametersResponse(
      SequencerConnect.GetSynchronizerParametersResponse.Parameters.ParametersV1(
        defaultStaticSynchronizerParameters.toProtoV30
      )
    )
  )

  private lazy val correctConnectionAttributes = ConnectionAttributes(
    testSynchronizerId(1),
    testSequencerId(1),
    defaultStaticSynchronizerParameters,
  )

  "SequencerConnectionX" should {
    "be validated in the happy path" in {
      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(successfulHandshake),
        synchronizerAndSeqIdResponses = Seq(correctSynchronizerIdResponse1),
        staticParametersResponses = Seq(correctStaticParametersResponse),
      )
      withConnection(responses) { connection => listener =>
        connection.start().valueOrFail("start connection")

        listener.shouldStabilizeOn(SequencerConnectionXState.Validated)
        connection.attributes shouldBe Some(correctConnectionAttributes)

        responses.assertAllResponsesSent()
      }
    }

    "refuse to start if it is in a fatal state" in {
      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(failedHandshake),
      )
      withConnection(responses) { connection => listener =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")

            listener.shouldStabilizeOn(SequencerConnectionXState.Fatal)
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include("Validation failure: Failed handshake"),
                "Handshake fails",
              )
            )
          ),
        )

        // Try to restart
        inside(connection.start()) {
          case Left(SequencerConnectionXError.InvalidStateError(message)) =>
            message shouldBe "The connection is in a fatal state and cannot be started"
        }

        responses.assertAllResponsesSent()
      }
    }

    "fail validation if the returned API is not for a sequencer" in {
      val responses = TestResponses(
        apiResponses = Seq(incorrectApiResponse)
      )
      withConnection(responses) { connection => listener =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionXState.Fatal)
            connection.attributes shouldBe None
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include("Validation failure: Bad API"),
                "API response is invalid",
              )
            )
          ),
        )

        responses.assertAllResponsesSent()
      }
    }

    "fail validation if the protocol handshake fails" in {
      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(failedHandshake),
      )
      withConnection(responses) { connection => listener =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionXState.Fatal)
            connection.attributes shouldBe None
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include("Validation failure: Failed handshake"),
                "Protocol handshake fails",
              )
            )
          ),
        )

        responses.assertAllResponsesSent()
      }
    }

    "retry if the server is unavailable during any request" in {
      val responses = TestResponses(
        apiResponses = Seq(failureUnavailable, correctApiResponse),
        handshakeResponses = Seq(failureUnavailable, successfulHandshake),
        synchronizerAndSeqIdResponses = Seq(failureUnavailable, correctSynchronizerIdResponse1),
        staticParametersResponses = Seq(failureUnavailable, correctStaticParametersResponse),
      )
      withConnection(responses) { connection => listener =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionXState.Validated)
            connection.attributes shouldBe Some(correctConnectionAttributes)
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include(
                  "Request failed for server-test-0. Is the server running?"
                ),
                "Request failure",
              )
            )
          ),
        )

        responses.assertAllResponsesSent()
      }
    }

    "validate the connection attributes after restart" in {
      val responses = TestResponses(
        apiResponses = Seq.fill(2)(correctApiResponse),
        handshakeResponses = Seq.fill(2)(successfulHandshake),
        synchronizerAndSeqIdResponses =
          Seq(correctSynchronizerIdResponse1, correctSynchronizerIdResponse2),
        staticParametersResponses = Seq.fill(2)(correctStaticParametersResponse),
      )
      withConnection(responses) { connection => listener =>
        connection.start().valueOrFail("start connection")
        listener.shouldStabilizeOn(SequencerConnectionXState.Validated)
        connection.attributes shouldBe Some(correctConnectionAttributes)

        listener.clear()
        connection.fail("test")
        listener.shouldStabilizeOn(SequencerConnectionXState.Stopped)
        listener.clear()

        // A different identity triggers a warning and the connection never gets validated
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionXState.Fatal)
            // Synchronizer info does not change
            connection.attributes shouldBe Some(correctConnectionAttributes)
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include("Sequencer connection has changed attributes"),
                "Different attributes after restart",
              )
            )
          ),
        )

        responses.assertAllResponsesSent()
      }
    }
  }

  private def withConnection[V](
      testResponses: TestResponses
  )(f: SequencerConnectionX => TestHealthListener => V): V = {
    val config = mkDummyConnectionConfig(0)

    val clientProtocolVersions = ProtocolVersionCompatibility.supportedProtocols(
      includeAlphaVersions = true,
      includeBetaVersions = true,
      release = ReleaseVersion.current,
    )
    val connection = GrpcSequencerConnectionX.create(
      config,
      clientProtocolVersions,
      minimumProtocolVersion = Some(testedProtocolVersion),
      clientFactory = new TestSequencerConnectionXClientFactory(testResponses),
      futureSupervisor,
      timeouts,
      loggerFactory,
    )

    val listener = new TestHealthListener(connection.health)
    connection.health.registerHighPriorityOnHealthChange(listener)

    ResourceUtil.withResource(connection)(f(_)(listener))
  }
}

final case class TestResponses(
    apiResponses: Seq[Either[Exception, v30.GetApiInfoResponse]] = Seq.empty,
    handshakeResponses: Seq[Either[Exception, SequencerConnect.HandshakeResponse]] = Seq.empty,
    synchronizerAndSeqIdResponses: Seq[
      Either[Exception, SequencerConnect.GetSynchronizerIdResponse]
    ] = Seq.empty,
    staticParametersResponses: Seq[
      Either[Exception, SequencerConnect.GetSynchronizerParametersResponse]
    ] = Seq.empty,
) extends Matchers {
  private val apiIndex = new AtomicInteger(0)
  private val handshakeIndex = new AtomicInteger(0)
  private val synchronizerAndSeqIdIndex = new AtomicInteger(0)
  private val staticParametersIndex = new AtomicInteger(0)

  val apiSvcFactory: Channel => ApiInfoServiceStub = channel =>
    new ApiInfoServiceStub(channel) {
      override def getApiInfo(request: v30.GetApiInfoRequest): Future[v30.GetApiInfoResponse] =
        nextResponse(apiIndex, apiResponses)

      override def build(channel: Channel, options: CallOptions): ApiInfoServiceStub = this
    }

  val sequencerConnectSvcFactory: Channel => SequencerConnectServiceStub =
    channel =>
      new SequencerConnectServiceStub(channel) {
        override def handshake(
            request: SequencerConnect.HandshakeRequest
        ): Future[SequencerConnect.HandshakeResponse] =
          nextResponse(handshakeIndex, handshakeResponses)

        override def getSynchronizerId(
            request: SequencerConnect.GetSynchronizerIdRequest
        ): Future[SequencerConnect.GetSynchronizerIdResponse] =
          nextResponse(synchronizerAndSeqIdIndex, synchronizerAndSeqIdResponses)

        override def getSynchronizerParameters(
            request: SequencerConnect.GetSynchronizerParametersRequest
        ): Future[SequencerConnect.GetSynchronizerParametersResponse] =
          nextResponse(staticParametersIndex, staticParametersResponses)

        override def build(channel: Channel, options: CallOptions): SequencerConnectServiceStub =
          this
      }

  private def nextResponse[T](
      atomicIndex: AtomicInteger,
      responses: Seq[Either[Exception, T]],
  ): Future[T] = {
    val index = atomicIndex.getAndIncrement()
    if (index < responses.size) responses(index).fold(Future.failed, Future.successful)
    else Future.failed(Status.UNAVAILABLE.asRuntimeException())
  }

  def assertAllResponsesSent(): Assertion = {
    withClue("API responses:")(apiIndex.get() shouldBe apiResponses.size)
    withClue("Handshake responses:")(handshakeIndex.get() shouldBe handshakeResponses.size)
    withClue("Synchronizer and sequencer ID responses:")(
      synchronizerAndSeqIdIndex.get() shouldBe synchronizerAndSeqIdResponses.size
    )
    withClue("Static synchronizer parameters responses:")(
      staticParametersIndex.get() shouldBe staticParametersResponses.size
    )
  }
}

class TestSequencerConnectionXClientFactory(testResponses: TestResponses)
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
