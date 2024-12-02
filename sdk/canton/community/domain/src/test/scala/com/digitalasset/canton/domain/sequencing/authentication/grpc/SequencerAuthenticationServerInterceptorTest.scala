// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication.grpc

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0.{Hello, HelloServiceGrpc}
import com.digitalasset.canton.domain.sequencing.authentication.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.authentication.grpc.{
  AuthenticationTokenManagerTest,
  AuthenticationTokenWithExpiry,
  SequencerClientTokenAuthentication,
}
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ServerInterceptors, Status}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration as JDuration
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
class SequencerAuthenticationServerInterceptorTest
    extends AnyWordSpec
    with BaseTest
    with BeforeAndAfterEach
    with HasExecutionContext {

  trait GrpcContext {
    class GrpcHelloService extends HelloServiceGrpc.HelloService {
      override def hello(request: Hello.Request): Future[Hello.Response] =
        Future.successful(Hello.Response("hello back"))

      override def helloStreamed(
          request: Hello.Request,
          responseObserver: StreamObserver[Hello.Response],
      ): Unit = ???
    }

    lazy val service = new GrpcHelloService()

    lazy val store: MemberAuthenticationStore = new MemberAuthenticationStore()
    lazy val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("popo::pipi"))

    lazy val authService = new MemberAuthenticationService(
      domainId,
      null,
      store,
      new SimClock(loggerFactory = loggerFactory),
      JDuration.ofMinutes(1),
      JDuration.ofHours(1),
      useExponentialRandomTokenExpiration = false,
      _ => (),
      FutureUnlessShutdown.unit,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    ) {
      override protected def isParticipantActive(participant: ParticipantId)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Boolean] = FutureUnlessShutdown.pure(true)
    }
    lazy val serverInterceptor =
      new SequencerAuthenticationServerInterceptor(authService, loggerFactory)

    val channelName = InProcessServerBuilder.generateName()
    val server = InProcessServerBuilder
      .forName(channelName)
      .addService(
        ServerInterceptors.intercept(
          HelloServiceGrpc.bindService(service, parallelExecutionContext),
          serverInterceptor,
        )
      )
      .build()
      .start()

    val participantId =
      UniqueIdentifier.fromProtoPrimitive_("p1::default").map(new ParticipantId(_)).value
    val neverExpire = CantonTimestamp.MaxValue
    val crypto = new SymbolicPureCrypto
    val token = AuthenticationTokenWithExpiry(AuthenticationToken.generate(crypto), neverExpire)
    val incorrectToken =
      AuthenticationTokenWithExpiry(AuthenticationToken.generate(crypto), neverExpire)

    require(token != incorrectToken, "The generated tokens must be different")
  }

  var channel: ManagedChannel = _
  override def afterEach(): Unit = {
    channel.shutdown()
    channel.awaitTermination(2, TimeUnit.SECONDS)
    channel.shutdownNow()
  }

  "Authentication interceptors" should {
    "fail request if client does not use interceptor to add auth metadata" in
      loggerFactory.suppressWarningsAndErrors(new GrpcContext {
        store
          .saveToken(StoredAuthenticationToken(participantId, neverExpire, token.token))

        channel = InProcessChannelBuilder.forName(channelName).build()
        val client = HelloServiceGrpc.stub(channel)

        inside(client.hello(Hello.Request("hi")).failed.futureValue) {
          case status: io.grpc.StatusRuntimeException =>
            status.getStatus.getCode shouldBe io.grpc.Status.UNAUTHENTICATED.getCode
        }
      })

    "succeed request if participant use interceptor with correct token information" in new GrpcContext {
      store
        .saveToken(StoredAuthenticationToken(participantId, neverExpire, token.token))

      val obtainToken = NonEmpty
        .mk(
          Seq,
          (
            Endpoint("localhost", Port.tryCreate(10)),
            (_ => EitherT.pure[FutureUnlessShutdown, Status](token)): TraceContext => EitherT[
              FutureUnlessShutdown,
              Status,
              AuthenticationTokenWithExpiry,
            ],
          ),
        )
        .toMap

      val clientAuthentication =
        SequencerClientTokenAuthentication(
          domainId,
          participantId,
          obtainToken,
          isClosed = false,
          AuthenticationTokenManagerConfig(),
          AuthenticationTokenManagerTest.mockClock,
          loggerFactory,
        )
      channel = InProcessChannelBuilder
        .forName(channelName)
        .build()
      val client = clientAuthentication(HelloServiceGrpc.stub(channel))
      client.hello(Hello.Request("hi")).futureValue.msg shouldBe "hello back"
    }

    "fail request if participant use interceptor with incorrect token information" in new GrpcContext {
      store
        .saveToken(StoredAuthenticationToken(participantId, neverExpire, token.token))

      val obtainToken = NonEmpty
        .mk(
          Seq,
          (
            Endpoint("localhost", Port.tryCreate(10)),
            (
                _ => EitherT.pure[FutureUnlessShutdown, Status](incorrectToken)
            ): TraceContext => EitherT[
              FutureUnlessShutdown,
              Status,
              AuthenticationTokenWithExpiry,
            ],
          ),
        )
        .toMap

      val clientAuthentication =
        SequencerClientTokenAuthentication(
          domainId,
          participantId,
          obtainToken,
          isClosed = false,
          AuthenticationTokenManagerConfig(),
          AuthenticationTokenManagerTest.mockClock,
          loggerFactory,
        )
      channel = InProcessChannelBuilder
        .forName(channelName)
        .build()
      val client = clientAuthentication(HelloServiceGrpc.stub(channel))

      inside(client.hello(Hello.Request("hi")).failed.futureValue) {
        case status: io.grpc.StatusRuntimeException =>
          status.getStatus.getCode shouldBe io.grpc.Status.UNAUTHENTICATED.getCode
      }
    }
  }
}
