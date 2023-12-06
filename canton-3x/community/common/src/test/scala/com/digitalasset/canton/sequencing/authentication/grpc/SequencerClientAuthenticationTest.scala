// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication.grpc

import cats.data.EitherT
import cats.implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.api.v0.{Hello, HelloServiceGrpc}
import com.digitalasset.canton.lifecycle.Lifecycle.CloseableChannel
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, DomainId, UniqueIdentifier}
import io.grpc.*
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.StreamObserver
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class SequencerClientAuthenticationTest extends FixtureAsyncWordSpec with BaseTest {

  val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("test::domain"))
  val participantId = DefaultTestIdentities.participant1
  val crypto = new SymbolicPureCrypto
  val token1 = AuthenticationToken.generate(crypto)
  val token2 = AuthenticationToken.generate(crypto)

  require(token1 != token2, "The generated tokens must be different")

  implicit val ec: ExecutionContext = executionContext

  type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env

    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  class Env extends AutoCloseable {
    val channelName = InProcessServerBuilder.generateName()
    val serverExpectedToken = new AtomicReference(token1)
    val clientNextTokenRefresh = new AtomicReference(token1)
    val authServerInterceptor = new AuthServerInterceptor(serverExpectedToken.get)
    val tokenManager =
      new AuthenticationTokenManager(
        _ =>
          EitherT.pure[Future, Status](
            AuthenticationTokenWithExpiry(clientNextTokenRefresh.get(), CantonTimestamp.Epoch)
          ),
        false,
        AuthenticationTokenManagerConfig(),
        AuthenticationTokenManagerTest.mockClock,
        loggerFactory,
      )
    val service = InProcessServerBuilder
      .forName(channelName)
      .addService(HelloServiceGrpc.bindService(new GrpcHelloService, executionContext))
      .intercept(authServerInterceptor)
      .build()

    service.start()

    val clientChannel =
      new CloseableChannel(
        InProcessChannelBuilder.forName(channelName).build(),
        logger,
        "auth-test-client-channel",
      )
    val managers = NonEmpty.mk(Seq, Endpoint("localhost", Port.tryCreate(10)) -> tokenManager).toMap
    val clientAuthentication =
      new SequencerClientTokenAuthentication(domainId, participantId, managers, loggerFactory)
    val client = HelloServiceGrpc
      .stub(clientChannel.channel)
      .withInterceptors(clientAuthentication.reauthorizationInterceptor)
      .withCallCredentials(clientAuthentication.callCredentials)

    override def close(): Unit = clientChannel.close()
  }

  "should refresh token after a failure" in { env =>
    import env.*

    val request = v0.Hello.Request(msg = "")
    for {
      _ <- client.hello(request)
      _ = {
        clientNextTokenRefresh.set(token2)
        authServerInterceptor.setValidToken(token2)
      }
      // first request should fail with an unauthenticated
      // (replaying is too much work and wouldn't work for streamed responses anyway)
      failure <- client.hello(request).failed
      _ = failure should matchPattern {
        case status: StatusRuntimeException
            if status.getStatus.getCode == Status.UNAUTHENTICATED.getCode =>
      }
      // the failure should have kicked off a token refresh, so this should work
      _ <- client.hello(request)
    } yield succeed
  }

  class AuthServerInterceptor(initialToken: AuthenticationToken) extends io.grpc.ServerInterceptor {
    private val validToken = new AtomicReference[AuthenticationToken](initialToken)
    override def interceptCall[ReqT, RespT](
        call: ServerCall[ReqT, RespT],
        headers: Metadata,
        next: ServerCallHandler[ReqT, RespT],
    ): ServerCall.Listener[ReqT] = {
      val providedToken = headers.get(Constant.AUTH_TOKEN_METADATA_KEY)

      if (providedToken != validToken.get()) {
        val returnedMetadata = new Metadata()
        returnedMetadata.put(Constant.AUTH_TOKEN_METADATA_KEY, providedToken)
        call.close(io.grpc.Status.UNAUTHENTICATED, returnedMetadata)
        new ServerCall.Listener[ReqT] {}
      } else {
        next.startCall(call, headers)
      }
    }

    def setValidToken(token: AuthenticationToken) = validToken.set(token)
  }

  class GrpcHelloService extends HelloServiceGrpc.HelloService {
    private val nextResponse = new AtomicReference(
      Future.successful(Hello.Response("well hey there"))
    )

    override def hello(request: Hello.Request): Future[Hello.Response] = nextResponse.get

    override def helloStreamed(
        request: Hello.Request,
        responseObserver: StreamObserver[Hello.Response],
    ): Unit = ???

    def setNextResponse(response: Future[Hello.Response]) = nextResponse.set(response)
    def setNextResponse(statusException: StatusException) =
      nextResponse.set(Future.failed(statusException))
  }
}
