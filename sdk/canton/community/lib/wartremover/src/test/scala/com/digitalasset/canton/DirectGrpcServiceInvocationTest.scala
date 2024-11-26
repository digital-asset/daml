// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import io.grpc.stub.AbstractStub
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.annotation.unused
import scala.concurrent.Future

class DirectGrpcServiceInvocationTest
    extends AnyWordSpec
    with Matchers
    with org.mockito.MockitoSugar {

  import DirectGrpcServiceInvocationTest.*

  "DirectGrpcServiceInvocation" should {

    "prevent direct calls to the stub's methods" in {
      val result = WartTestTraverser(DirectGrpcServiceInvocation) {
        val service = ??? : MyServiceStub
        val request = ??? : MyServiceRequest
        service.myServiceRequest(request)
      }

      result.errors.length shouldBe 1
    }

    "allow calls to the stub's method that go through our annotated helper methods" in {
      val result = WartTestTraverser(DirectGrpcServiceInvocation) {
        val service = ??? : MyServiceStub
        val request = ??? : MyServiceRequest

        MyGrpcUtil.send1(service, ())(_.myServiceRequest(request))

        implicit val i: Int = 1
        MyGrpcUtil.send2(service)(_.myServiceRequest(request), ())
      }
      result.errors shouldBe empty
    }

    "allow calls to the stub's configuration methods" in {
      val result = WartTestTraverser(DirectGrpcServiceInvocation) {
        val service = ??? : MyServiceStub
        // All the unary methods of AbstractStub
        service
          .withDeadline(null)
          .withExecutor(null)
          .withCompression("")
          .withChannel(null)
          .withInterceptors(null)
          .withCallCredentials(null)
          .withMaxInboundMessageSize(0)
          .withMaxOutboundMessageSize(0)
      }
      result.errors shouldBe empty
    }

    "allow equality comparisons" in {
      val result = WartTestTraverser(DirectGrpcServiceInvocation) {
        val service = ??? : MyServiceStub
        service.equals(service)
        service == service
        service eq service
      }
      result.errors shouldBe empty
    }

    "honor annotated constructors" in {
      val result = WartTestTraverser(DirectGrpcServiceInvocation) {
        val service = ??? : MyServiceStub
        val request = ??? : MyServiceRequest

        new MyGrpcUtil(service.myServiceRequest(request))
        0
      }
      result.errors shouldBe empty

    }

    "do not look into annotated methods" in {
      val result = WartTestTraverser(DirectGrpcServiceInvocation) {

        val service = ??? : MyServiceStub

        class Parent {
          @GrpcServiceInvocationMethod
          def myMethod(request: MyServiceRequest): Future[MyServiceResponse] =
            service.myServiceRequest(request)
        }

        class Child extends Parent {
          override def myMethod(request: MyServiceRequest): Future[MyServiceResponse] =
            service.myServiceRequest(request)
        }

        class Grandchild extends Child {
          override def myMethod(request: MyServiceRequest): Future[MyServiceResponse] =
            service.myServiceRequest(request)
        }

        new Grandchild
      }
      result.errors shouldBe empty
    }

    "handle named arguments that come out of order" in {
      val result = WartTestTraverser(DirectGrpcServiceInvocation) {
        val service = ??? : MyServiceStub
        val request = ??? : MyServiceRequest

        MyGrpcUtil.send1(moreArgs = (), stub = service)(_.myServiceRequest(request))

        implicit val i: Int = 1
        // Named arguments are out of order compared to the method definition
        MyGrpcUtil.send2(service)(
          moreArgs = (),
          send = _.myServiceRequest(request),
        )
      }

      result.errors shouldBe empty
    }

    "handle omitted default arguments" in {
      val result = WartTestTraverser(DirectGrpcServiceInvocation) {
        val service = ??? : MyServiceStub
        val request = ??? : MyServiceRequest

        MyGrpcUtil.sendWithDefaultArgs(service)(_.myServiceRequest(request), defaultArg2 = 0)
      }

      result.errors shouldBe empty
    }
  }
}

object DirectGrpcServiceInvocationTest {
  private trait MyServiceRequest
  private trait MyServiceResponse

  private class MyServiceStub extends AbstractStub[MyServiceStub](null, null) {
    def myServiceRequest(request: MyServiceRequest): Future[MyServiceResponse] = ???
    override def build(channel: io.grpc.Channel, options: io.grpc.CallOptions): MyServiceStub = ???
  }

  private object MyGrpcUtil {
    @GrpcServiceInvocationMethod
    def send1[Svc <: AbstractStub[Svc], Resp](stub: Svc, moreArgs: Unit)(
        send: Svc => Future[Resp]
    ): Future[Resp] = ???

    @GrpcServiceInvocationMethod
    def send2[Svc <: AbstractStub[Svc], Resp](stub: Svc)(send: Svc => Future[Resp], moreArgs: Unit)(
        implicit implicitArg: Int
    ): Future[Resp] = ???

    @GrpcServiceInvocationMethod
    def sendWithDefaultArgs[Svc <: AbstractStub[Svc], Resp](stub: Svc)(
        send: Svc => Future[Resp],
        defaultArg1: Int = 1,
        defaultArg2: Int = 2,
    ): Future[Resp] = ???

    @GrpcServiceInvocationMethod
    def sendInOneArgList[Svc <: AbstractStub[Svc], Resp](
        stub: Svc,
        send: Svc => Future[Resp],
    ): Future[Resp] = ???
  }

  @GrpcServiceInvocationMethod
  private class MyGrpcUtil(
      @unused
      arg: Any
  )
}
