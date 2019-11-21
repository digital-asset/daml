// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.reflection

import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.platform.sandbox.services.SandboxFixture
import io.grpc.reflection.v1alpha.{
  ServerReflectionGrpc,
  ServerReflectionRequest,
  ServerReflectionResponse
}
import io.grpc.stub.StreamObserver
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

final class ReflectionIT
    extends AsyncWordSpec
    with Matchers
    with SandboxFixture
    with SuiteResourceManagementAroundAll {

  private val listServices: ServerReflectionRequest =
    ServerReflectionRequest.newBuilder().setHost("127.0.0.1").setListServices("").build()

  "Reflection service" when {

    "accessed" should {

      "provide a list of exposed services" in {
        for {
          response <- execRequest(listServices)
        } yield {
          response.getListServicesResponse.getServiceCount shouldEqual 13
        }
      }

      "provide details about each service" in {
        for {
          servicesResponse <- execRequest(listServices)
          symbolResponses <- Future.sequence {
            servicesResponse.getListServicesResponse.getServiceList.asScala
              .map(resp => execRequest(fileBySymbolReq(resp.getName)))
          }
        } yield {
          for {
            r <- symbolResponses
            p <- r.getFileDescriptorResponse.getFileDescriptorProtoList.asScala
          } {
            // We filter for this string due to an exotic bug in the bazel-grpc setup, see grpc-definitions/BUILD.bazel.
            assert(
              !p.toStringUtf8.contains("bazel-out"),
              s"filedescriptor ${p.toStringUtf8} contains string 'bazel-out'. This means grpc reflection will not work.")
          }
          all(symbolResponses) should have('hasErrorResponse (false))
        }
      }
    }
  }

  private def fileBySymbolReq(symbol: String) =
    ServerReflectionRequest.newBuilder().setFileContainingSymbol(symbol).build()

  private def execRequest(request: ServerReflectionRequest) = {
    val doneP = Promise[ServerReflectionResponse]()
    val ro =
      ServerReflectionGrpc
        .newStub(channel)
        .serverReflectionInfo(new StreamObserver[ServerReflectionResponse] {
          override def onNext(v: ServerReflectionResponse): Unit = {
            doneP.success(v)
          }

          override def onError(throwable: Throwable): Unit = doneP.failure(throwable)

          override def onCompleted(): Unit = {
            doneP.tryFailure(new NoSuchElementException("Stream closed without any response."))
            ()
          }
        })
    ro.onNext(request)
    ro.onCompleted()
    val doneF = doneP.future
    doneF
  }
}
