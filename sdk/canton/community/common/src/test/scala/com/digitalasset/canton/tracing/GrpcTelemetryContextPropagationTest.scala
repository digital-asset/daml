// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.digitalasset.canton.domain.api.v0.Hello
import com.digitalasset.canton.domain.api.v0.HelloServiceGrpc.HelloService
import com.digitalasset.canton.networking.grpc.CantonGrpcUtilTest
import com.digitalasset.canton.tracing.TestTelemetry.eventsOrderedByTime
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.stub.StreamObserver
import io.opentelemetry.api.trace.Tracer
import org.scalatest.Outcome
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=match may not be exhaustive")
class GrpcTelemetryContextPropagationTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext
    with Eventually {
  val request: Hello.Request = CantonGrpcUtilTest.request
  val response: Hello.Response = CantonGrpcUtilTest.response

  class MyHelloService(implicit tracer: Tracer) extends HelloService with Spanning {
    override def hello(request: Hello.Request): Future[Hello.Response] =
      withSpanFromGrpcContext("MyHelloService.hello") { _ => span =>
        Future.successful {
          span.addEvent("ran MyHelloService.hello")
          response
        }
      }
    override def helloStreamed(
        request: Hello.Request,
        responseObserver: StreamObserver[Hello.Response],
    ): Unit = ()
  }

  class Outer()(implicit tracer: Tracer) extends Spanning {
    def foo[A](f: TraceContext => Future[A]): Future[A] =
      withNewTrace("Outer.foo") { implicit traceContext => span =>
        span.addEvent("running Outer.foo")
        f(traceContext).map { r =>
          span.addEvent("finished Outer.foo")
          r
        }
      }
  }

  override type FixtureParam = Env

  class Env {
    val telemetry = new TestTelemetrySetup()
    implicit val tracer: Tracer = telemetry.tracer
    val grpc = new CantonGrpcUtilTest.Env(new MyHelloService(), logger)(parallelExecutionContext)

    def close(): Unit = {
      grpc.close()
      telemetry.close()
    }
  }

  override def withFixture(test: OneArgTest): Outcome = {
    val env = new Env()
    try {
      withFixture(test.toNoArgTest(env))
    } finally {
      env.close()
    }
  }

  "The telemetry context" should {
    "be propagated from GRPC client to server" in { env =>
      import env.*
      implicit val tracer: Tracer = telemetry.tracer

      val sut = new Outer()
      grpc.server.start()

      sut.foo(grpc.sendRequest()(_).value).futureValue shouldBe Right(response)

      eventually { telemetry.reportedSpans() should have size (2) }

      val List(rootSpan, childSpan) = telemetry.reportedSpans()
      childSpan.getParentSpanId shouldBe rootSpan.getSpanId
      childSpan.getTraceId shouldBe rootSpan.getTraceId
      rootSpan.getName shouldBe "Outer.foo"
      childSpan.getName shouldBe "MyHelloService.hello"

      eventsOrderedByTime(rootSpan, childSpan).map(_.getName) should contain.inOrderOnly(
        "running Outer.foo",
        "ran MyHelloService.hello",
        "finished Outer.foo",
      )
    }
  }
}
