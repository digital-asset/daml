// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.reflection

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.testing.StreamConsumer
import io.grpc.reflection.v1alpha.{
  ServerReflectionGrpc,
  ServerReflectionRequest,
  ServerReflectionResponse,
}
import org.scalatest.Inspectors._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.jdk.CollectionConverters._
import scala.concurrent.Future

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
        val expectedServiceCount: Int = 19
        for {
          response <- execRequest(listServices)
        } yield {
          withClue("ServiceCount: ") {
            response.getListServicesResponse.getServiceCount shouldEqual expectedServiceCount
          }
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
              s"filedescriptor ${p.toStringUtf8} contains string 'bazel-out'. This means grpc reflection will not work.",
            )
          }
          forAll(symbolResponses) { r =>
            r.hasErrorResponse should be(false)
          }
        }
      }
    }
  }

  private def fileBySymbolReq(symbol: String) =
    ServerReflectionRequest.newBuilder().setFileContainingSymbol(symbol).build()

  private def execRequest(request: ServerReflectionRequest) =
    new StreamConsumer[ServerReflectionResponse](
      ServerReflectionGrpc
        .newStub(channel)
        .serverReflectionInfo(_)
        .onNext(request)
    ).first().map(_.get)

}
