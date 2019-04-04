// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture}
import io.grpc.reflection.v1alpha.{ServerReflectionRequest, ServerReflectionResponse}
import io.grpc.stub.StreamObserver
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.convert.ImplicitConversionsToScala._
import scala.concurrent.{Future, Promise}

class ReflectionIT
    extends AsyncWordSpec
    with Matchers
    with MultiLedgerFixture
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundAll {
  override protected def config: Config = Config.default

  def listServices =
    ServerReflectionRequest.newBuilder().setHost("127.0.0.1").setListServices("").build()

  "Reflection service" when {

    "accessed" should {

      "provide a list of exposed services" in allFixtures { ledger =>
        for {
          response <- execRequest(ledger, listServices)
        } yield {
          response.getListServicesResponse.getServiceCount shouldEqual 11
        }
      }

      "provide details about each service" in allFixtures { ledger =>
        for {
          servicesResponse <- execRequest(ledger, listServices)
          symbolResponses <- Future.sequence {
            servicesResponse.getListServicesResponse.getServiceList
              .map(resp => execRequest(ledger, fileBySymbolReq(resp.getName)))
          }
        } yield {
          for {
            r <- symbolResponses
            p <- r.getFileDescriptorResponse.getFileDescriptorProtoList
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

  private def fileBySymbolReq(symbol: String) = {
    val request =
      ServerReflectionRequest.newBuilder().setFileContainingSymbol(symbol).build()
    request
  }

  private def getFileRequest(fileName: String) = {
    ServerReflectionRequest
      .newBuilder()
      .setFileByFilename(fileName)
      .build()
  }

  private def execRequest(ledger: LedgerContext, request: ServerReflectionRequest) = {
    val doneP = Promise[ServerReflectionResponse]()
    val ro =
      ledger.reflectionService.serverReflectionInfo(new StreamObserver[ServerReflectionResponse] {
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
