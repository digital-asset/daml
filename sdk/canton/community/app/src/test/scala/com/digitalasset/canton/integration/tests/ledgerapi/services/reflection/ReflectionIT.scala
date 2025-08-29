// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services.reflection

import com.daml.grpc.test.StreamConsumer
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import io.grpc.reflection.v1.{
  ServerReflectionGrpc,
  ServerReflectionRequest,
  ServerReflectionResponse,
}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

final class ReflectionIT extends CantonFixture {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val listServices: ServerReflectionRequest =
    ServerReflectionRequest.newBuilder().setHost("127.0.0.1").setListServices("").build()

  "Reflection service" when {

    "accessed" should {

      "provide a list of exposed services" in { env =>
        import env.*
        val expectedServiceCount: Int = 19
        (for {
          response <- execRequest(listServices)
        } yield {
          withClue("ServiceCount: ") {
            response.getListServicesResponse.getServiceCount shouldEqual expectedServiceCount
          }
        }).futureValue
      }

      "provide details about each service" in { env =>
        import env.*
        (for {
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
        }).futureValue
      }
    }
  }

  private def fileBySymbolReq(symbol: String) =
    ServerReflectionRequest.newBuilder().setFileContainingSymbol(symbol).build()

  private def execRequest(request: ServerReflectionRequest)(implicit ec: ExecutionContext) =
    new StreamConsumer[ServerReflectionResponse](
      ServerReflectionGrpc
        .newStub(channel)
        .serverReflectionInfo(_)
        .onNext(request)
    ).first().map(_.value)

}
